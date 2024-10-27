use crate::heap_page::{self, HeapPage};
use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::{fs, num};

pub const STORAGE_DIR: &str = "heapstore";

// The data types we need for tracking the mapping between containerId and HeapFile/PathBuf
pub(crate) type ContainerMap = Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>;
pub(crate) type ContainerPathMap = Arc<RwLock<HashMap<ContainerId, Arc<PathBuf>>>>;
const PERSIST_CONFIG_FILENAME: &str = "storage_manager";

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_dir: PathBuf,
    /// Indicates if this is a temp StorageManager (for testing)
    is_temp: bool,
    pub(crate) cid_path_map: ContainerPathMap,
    #[serde(skip)]
    pub(crate) cid_heapfile_map: ContainerMap,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {

    /// Get a page if exists for a given container.
    pub(crate) fn get_page( //locate a page with a heapfile id and page id (file 4 page 3)
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        //Lock the RwLock on the cid_heapfile_map to access the HashMap
        let heapfile_map = self.cid_heapfile_map.read().unwrap();

        //Look up the heap file in the HashMap using container_id
        let heap_file = heapfile_map.get(&container_id)?.clone();  // Clone the Arc<HeapFile>

        //Read the page from the heap file using the page_id
        let page = heap_file.read_page_from_file(page_id).ok()?;

        //Return the page if found
        Some(page)
    }

    /// Write a page
    pub(crate) fn write_page( //locate page with heapfile id and page id and replace that page
        &self,
        container_id: ContainerId,
        page: &Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        //Lock the RwLock on the cid_heapfile_map to access the HashMap
        let heapfile_map = self.cid_heapfile_map.read().unwrap();

        //Look up the heap file in the HashMap using container_id
        let heap_file = heapfile_map.get(&container_id)
            .ok_or_else(|| CrustyError::CrustyError("HeapFile not found".to_string()))?
            .clone();  // Clone the Arc<HeapFile> to get shared ownership

        //Write the page to the heap file using page_id
        heap_file.write_page_to_file(page)
            .map_err(|e| CrustyError::CrustyError(format!("Failed to write page: {:?}", e)))?;

        //Return Ok to indicate success
        Ok(())
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        //Lock the RwLock on the cid_heapfile_map to access the HashMap
        let heapfile_map = self.cid_heapfile_map.read().unwrap();

        //Look up the heap file in the HashMap using container_id
        if let Some(heap_file) = heapfile_map.get(&container_id) {
            //Return the number of pages by calling num_pages on the HeapFile
            heap_file.num_pages()
        } else {
            //If the heap file doesn't exist, return 0
            0
        }
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let heapfile_map = self.cid_heapfile_map.read().unwrap();
    
        if let Some(heap_file) = heapfile_map.get(&container_id) {
            // Fetch the value of AtomicU16
            let read_count = heap_file.read_count.load(Ordering::Relaxed);
            let write_count = heap_file.write_count.load(Ordering::Relaxed);
            (read_count, write_count)
        } else {
            // If the heap file doesn't exist, return (0, 0)
            (0, 0)
        }
    }
    

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_dir as the location to persist data
    /// (if the storage manager persists records on disk)
    /// For startup/shutdown: check the storage_dir for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_dir: &Path) -> Self {
        let sm_file = storage_dir;
        let sm_file = sm_file.join(PERSIST_CONFIG_FILENAME);
        if sm_file.exists() {
            debug!("Loading storage manager from config file {:?}", sm_file);
            let reader = fs::File::open(sm_file).expect("error opening persist config file");
            let sm: StorageManager =
                serde_json::from_reader(reader).expect("error reading from json");
            
            let mut hm: HashMap<ContainerId, Arc<HeapFile>> = HashMap::new();
            let mut hmfiles: HashMap<ContainerId, Arc<PathBuf>> = HashMap::new();

            let path_map: ContainerPathMap = sm.cid_path_map.clone();
            let old_files = path_map.read().unwrap();

            for (id, path) in old_files.iter() {
                let hf = HeapFile::new(path.to_path_buf(), *id)
                    .expect("Error creating/opening old HF {path}");
                hmfiles.insert(*id, Arc::new(path.to_path_buf()));
                hm.insert(*id, Arc::new(hf));
            }

            let cid_heapfile_map = Arc::new(RwLock::new(hm));
            let cid_path_map = Arc::new(RwLock::new(hmfiles));
            StorageManager {
                storage_dir: storage_dir.to_path_buf(),
                cid_heapfile_map,
                cid_path_map,
                is_temp: false,
            }
        } else {
            debug!("Making new storage_manager in directory {:?}", storage_dir);

            //Create empty ContainerMap and ContainerPathMap
            let cid_heapfile_map = Arc::new(RwLock::new(HashMap::new()));
            let cid_path_map = Arc::new(RwLock::new(HashMap::new()));

            // Step 2: Construct the new StorageManager with empty mappings
            StorageManager {
                storage_dir: storage_dir.to_path_buf(),
                cid_heapfile_map,
                cid_path_map,
                is_temp: false,
            }
        }
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_dir = gen_random_test_sm_dir();
        debug!("Making new temp storage_manager {:?}", storage_dir);

        //Create empty ContainerMap and ContainerPathMap
        let cid_heapfile_map = Arc::new(RwLock::new(HashMap::new()));
        let cid_path_map = Arc::new(RwLock::new(HashMap::new()));

        //Construct and return the StorageManager with is_temp set to true
        StorageManager {
            storage_dir,
            cid_heapfile_map,
            cid_path_map,
            is_temp: true,
        }
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        let heapfile_map = self.cid_heapfile_map.read().unwrap(); //get map of heapfiles

        let heap_file = heapfile_map.get(&container_id) //find heapfile we want
            .expect("HeapFile not found")
            .clone();  // Clone the Arc<HeapFile>

        let num_pages = heap_file.num_pages();
        
        for page_id in 0..num_pages { //check each page in this heapfile
            
            let mut page = heap_file.read_page_from_file(page_id).expect("Failed to read page"); //get one page
            
            if page.get_free_space() >= value.len() { //check if there's enough free space in this page
                
                //insert the value into the page
                let slot_id = page.add_value(&value).expect("Failed to insert value");

                //2rite the updated page back to the heap file
                heap_file.write_page_to_file(&page).expect("Failed to write page");

                //return the ValueId for the inserted value
                return ValueId::new_slot(container_id, page_id, slot_id); //possibly a problem!
            }
        }
        //if we get here, we did not find a page with enough space


        //when we insert a tuple, we need to know which heap file its in, which page its on and which slot its in

        //POSSIBLE ISSUE - NOT INCREMENTING NUM PAGES IN NEW FILE, MIGHT BE TAKEN CARE OF AUTOMATICALLY
        let new_page_id = num_pages;  //assign the new page_id to the next available page
        let mut new_page = Page::new(new_page_id);

        //insert the value into the new page
        let slot_id = new_page.add_value(&value).expect("Failed to insert value into new page");

        //write the new page to the heap file
        heap_file.write_page_to_file(&new_page).expect("Failed to write new page");

        //return the value id for the inserted value in the new page
        ValueId::new_slot(container_id, new_page_id, slot_id)

    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {

        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = heapfile_map.get(&id.container_id) //get heap file that holds this tuple
            .ok_or_else(|| CrustyError::CrustyError("HeapFile not found".to_string()))?
            .clone();  // Clone the Arc<HeapFile>

        let mut page = heap_file.read_page_from_file(id.page_id.unwrap()) //get page of the heap file that holds tuple
            .map_err(|_| CrustyError::CrustyError("Failed to read page".to_string()))?;
        
        page.delete_value(id.slot_id.unwrap()) //delete whatever is in the slot with given id
            .ok_or_else(|| CrustyError::CrustyError("Failed to delete value".to_string()))?;

        //write the updated page back to the heap file
        heap_file.write_page_to_file(&page)
            .map_err(|_| CrustyError::CrustyError("Failed to write updated page".to_string()))?;

        //return Ok if successful
        Ok(())
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        
        //cheack that the new value size does not exceed the page size
        if value.len() > PAGE_SIZE {
            return Err(CrustyError::CrustyError("Value size exceeds page size".to_string()));
        }

        //get the heapfile that holds this tuple
        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = heapfile_map.get(&id.container_id)
            .ok_or_else(|| CrustyError::CrustyError("HeapFile not found".to_string()))?
            .clone();  // Clone the Arc<HeapFile>

        //read the page that holds this tuple
        let mut page = heap_file.read_page_from_file(id.page_id.unwrap())
            .map_err(|_| CrustyError::CrustyError("Failed to read page".to_string()))?;

        //delete the old value from the page using slot_id
        page.delete_value(id.slot_id.unwrap())
            .ok_or_else(|| CrustyError::CrustyError("Failed to delete value".to_string()))?;

        //attempt to add the new value to the page
        if let Some(new_slot_id) = page.add_value(&value) {
            // Step 6: Write the updated page back to the heap file
            heap_file.write_page_to_file(&page)
                .map_err(|_| CrustyError::CrustyError("Failed to write updated page".to_string()))?;

            //return the new ValueId with the updated slot
            Ok(ValueId::new_slot(id.container_id, id.page_id.unwrap(), new_slot_id))
        } else {
            //if adding the new value fails, return an error
            Err(CrustyError::CrustyError("Failed to add new value; insufficient space".to_string()))
        }
    }

    /// Create a new container (i.e., a HeapFile) to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {

        let file_path = self.storage_dir.join(format!("container_{}.db", container_id));
        
        let heap_file = HeapFile::new(file_path.clone(), container_id) //create the heapfile
            .map_err(|e| CrustyError::CrustyError(format!("Failed to create HeapFile: {:?}", e)))?;

        //put this heapfile into the map
        let mut heapfile_map = self.cid_heapfile_map.write().unwrap();
        heapfile_map.insert(container_id, Arc::new(heap_file));

        //put this file path into the path map
        let mut path_map = self.cid_path_map.write().unwrap();
        path_map.insert(container_id, Arc::new(file_path));

        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, common::ids::StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted, remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        
        //get the file path to this heapfile
        let mut path_map = self.cid_path_map.write().unwrap();
        let file_path = path_map.remove(&container_id)
            .ok_or_else(|| CrustyError::CrustyError("Container path not found".to_string()))?;

        //remove heapfile from map
        let mut heapfile_map = self.cid_heapfile_map.write().unwrap();
        heapfile_map.remove(&container_id);

        //delete the underlying file from disk
        if let Err(e) = fs::remove_file(&*file_path) {
            return Err(CrustyError::CrustyError(format!("Failed to delete file: {:?}", e)));
        }

        //Return Ok if container removal is successful
        Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        
        //get the heapfile with given id
        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = heapfile_map.get(&container_id)
            .expect("HeapFile not found")
            .clone();  // Clone the Arc<HeapFile> for shared ownership
        
        //create an iterator for this heapfile
        HeapFileIterator::new(tid, heap_file)
    }

    fn get_iterator_from(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
        start: ValueId,
    ) -> Self::ValIterator {
        
        //get the heapfile with given id
        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = heapfile_map.get(&container_id)
            .expect("HeapFile not found")
            .clone();

        //create an iterator that starts at a specified start id
        HeapFileIterator::new_from(tid, heap_file, start)
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        
        //get the heapfile that holds this tuple
        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = heapfile_map.get(&id.container_id)
            .ok_or_else(|| CrustyError::CrustyError("HeapFile not found".to_string()))?
            .clone();  // Clone the Arc<HeapFile>

        //get the page that holds this tuple
        let page = heap_file.read_page_from_file(id.page_id.unwrap())
            .map_err(|_| CrustyError::CrustyError("Failed to read page".to_string()))?;

        //get the tuple from from the page
        let value = page.get_value(id.slot_id.unwrap())
            .ok_or_else(|| CrustyError::CrustyError("Value not found in specified slot".to_string()))?;

        //return the tuple as a Vec<u8>
        Ok(value)
    }

    fn get_storage_path(&self) -> &Path {
        &self.storage_dir
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_dir.clone())?;
        fs::create_dir_all(self.storage_dir.clone()).unwrap();
        
        // Step 2: Clear internal data structures
        // Acquire write locks on cid_heapfile_map and cid_path_map, then clear them
        {
            let mut heapfile_map = self.cid_heapfile_map.write().unwrap();
            heapfile_map.clear();
        }

        {
            let mut path_map = self.cid_path_map.write().unwrap();
            path_map.clear();
        }

        // Return Ok to indicate reset was successful
        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {}

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        debug!("serializing storage manager");
        let mut filename = self.storage_dir.clone();
        filename.push(PERSIST_CONFIG_FILENAME);
        serde_json::to_writer(
            fs::File::create(filename).expect("error creating file"),
            &self,
        )
        .expect("error serializing storage manager");
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_dir);
            let remove_all = fs::remove_dir_all(self.storage_dir.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}


#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}


