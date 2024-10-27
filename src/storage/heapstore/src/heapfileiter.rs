use crate::heap_page::HeapPage;
use crate::heap_page::HeapPageIntoIter;
use crate::heapfile::HeapFile;
use common::prelude::*;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    current_page_id: PageId,               // Current page being iterated over
    current_page_iter: Option<HeapPageIntoIter>,  // Current page's slot iterator
    heapfile: Arc<HeapFile>,               // Reference to the heap file
    tid: TransactionId,                    // The transaction ID for safety
}//TODO milestone hs


/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let first_page_id = 0; // Start from the first page

        // Read the first page from the heap file
        let first_page = hf.read_page_from_file(first_page_id).unwrap(); // Handle error appropriately
        let page_iter = first_page.into_iter();

        Self {
            current_page_id: first_page_id,
            current_page_iter: Some(page_iter),
            heapfile: hf,
            tid,
        }
    }

    pub(crate) fn new_from(tid: TransactionId, hf: Arc<HeapFile>, value_id: ValueId) -> Self {
        // Assume value_id contains the information for both page and slot
        let page_id = value_id.page_id.unwrap();  // Unwrap or handle Option
        let start_slot = value_id.slot_id.unwrap();  // Unwrap or handle Option
    
        // Read the page starting from the value_id's page
        let page = hf.read_page_from_file(page_id).unwrap();  // Handle error appropriately
        let mut page_iter = page.into_iter();  // Create the iterator for the page
    
        // Advance the page iterator to the correct slot
        page_iter.nth(start_slot as usize);  // Skip to the correct slot
    
        Self {
            current_page_id: page_id,  // Use the unwrapped page_id
            current_page_iter: Some(page_iter),  // Use page_iter here
            heapfile: hf,
            tid,
        }
    }
    

}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);

    fn next(&mut self) -> Option<Self::Item> {

        loop {
            // If there's a current page iterator, try to get the next value
            if let Some(ref mut page_iter) = self.current_page_iter {
                if let Some((value, slot_id)) = page_iter.next() {
                    
                    // Create the ValueId using current_page_id and slot_id
                    let value_id = ValueId::new_slot(self.heapfile.container_id, self.current_page_id, slot_id);
                    return Some((value, value_id));
                }
            }

            //when we insert a tuple, we need to know which heap file its in, which page its on and which slot its in

            // If current page iterator is exhausted or missing, move to the next page
            self.current_page_id += 1;

            // Check if we have more pages in the heap file
            if self.current_page_id < self.heapfile.num_pages() {
                // Load the next page and create a new iterator for it
                let next_page = self.heapfile.read_page_from_file(self.current_page_id).ok()?;
                self.current_page_iter = Some(next_page.into_iter());
            } else {
                // No more pages to iterate over
                return None;
            }
        }
    }
}
