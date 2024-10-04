use crate::page;
use crate::page::{Offset, Page};
use common::crusty_graph::NodeIndex;
use common::{prelude::*, query_result};
use common::PAGE_SIZE;
use std::fmt;
use std::fmt::Write;
// todo!("Add any other imports you need here")

// Add any other constants, type aliases, or structs, or definitions here
pub const FIXED_HEADER_SIZE: usize = 8;
pub const SLOT_SIZE: u16 = 6;
pub const PAGE_ID_0: usize = 0;
pub const PAGE_ID_1: usize = 1;
pub const NUM_SLOTS_0: usize = 2;
pub const NUM_SLOTS_1: usize = 3;
pub const FS_OFFSET_0: usize = 4;
pub const FS_OFFSET_1: usize = 5;
pub const FS_SIZE_0: usize = 6;
pub const FS_SIZE_1: usize = 7;
pub const INVALID_OFFSET: u16 = 0;

// layout of page metadata (first 8 bytes) in page header
// [pageID, pageID, num slots, num slots, fs offset, fs offset, fs size, fs size]  

pub trait HeapPage {
    // Do not change these functions signatures (only the function bodies)
    fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId>;
    fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>>;
    fn delete_value(&mut self, slot_id: SlotId) -> Option<()>;
    fn get_header_size(&self) -> usize;
    fn get_free_space(&self) -> usize;

    //Add function signatures for any helper function you need here
    fn update_tuple_offset(&mut self, slot_id: SlotId, offset: Offset);
    fn reclaim_free_space_header(&mut self, freed_size: usize);
    fn update_slots_after_deletion(&mut self, deleted_offset: usize, deleted_size: usize);
    fn mark_slot_as_free(&mut self, slot_id: SlotId);
    fn get_slot_metadata(&self, slot_id: SlotId) -> Option<(Offset, usize)>;
    fn add_tuple_metadata(&mut self, slot_id: SlotId, offset: Offset, value_size: usize);
    fn compact_free_space(&mut self, deleted_offset: usize, deleted_size: usize);
    fn get_slot_offset(&self, slot_id: SlotId) -> Offset;
    fn get_num_slots(&self) -> usize;
    fn find_free_slot(&self) -> Option<SlotId>;
    fn combine_u8s_to_u16_le(arr: &[u8], idx: usize) -> u16;
    fn update_slot_counter(&mut self, num_slots: u16);
}

impl HeapPage for Page {
    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should reuse the slotId in the future.
    /// The page should always assign the lowest available slot_id to an insertion.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> { 
        let value_size = bytes.len();
        let free_space = self.get_free_space();
        let total_needed = value_size + SLOT_SIZE as usize;

        // Check if there is enough space for the new slot and the value
        if free_space < total_needed {
            println!("not enough space foe new item and it's slot!");
            return None;
        }

        // Try to find an existing free slot, or add a new one if there's enough space
        let slot_id = if let Some(free_slot) = self.find_free_slot() {
            free_slot
        } else {
            // No free slot found, add a new slot
            let num_slots = self.get_num_slots();
            let new_header_size = FIXED_HEADER_SIZE + (num_slots + 1) * SLOT_SIZE as usize;

            if new_header_size + value_size > PAGE_SIZE {
                return None; // Not enough space for a new slot and the value
            }

            // Increment the number of slots
            let new_num_slots = num_slots + 1;
            self.update_slot_counter(new_num_slots as u16);

            (new_num_slots - 1) as u16 // Return the next available slot ID
        };

        // Find free space on the other end of the page (the "right side")
        let free_offset = Self::combine_u8s_to_u16_le(&self.data, FS_OFFSET_0 as usize) as usize;

        // Write the data to the page
        let end = PAGE_SIZE - free_offset; //subtract with overflow?
        let start = end - value_size;
        self.data[start..end].clone_from_slice(bytes);

        // Update the header
        self.add_tuple_metadata(slot_id, start as Offset, value_size);

        Some(slot_id)
    }

    // Updates the slot counter stored in indices 2 and 3 of the data array.
    fn update_slot_counter(&mut self, num_slots: u16) {
        let num_slots_bytes = num_slots.to_le_bytes();
        self.data[NUM_SLOTS_0..NUM_SLOTS_1 + 1].copy_from_slice(&num_slots_bytes);
    }

    // Retrieves the current number of slots stored in indices 2 and 3 of the data array.
    fn get_num_slots(&self) -> usize {
        u16::from_le_bytes(self.data[2..4].try_into().unwrap()) as usize
    }

    // add tuple metadata to an open slot or add a new slot and update free space info
    fn add_tuple_metadata(&mut self, slot_id: SlotId, offset: Offset, value_size: usize) {
        
        //calculate start of the slot
        let slot_start = FIXED_HEADER_SIZE + (slot_id * SLOT_SIZE) as usize; // Header starts at byte 8, each slot takes 6 bytes
        let tuple_offset_start = slot_start + 2;
        let slot_end = tuple_offset_start + 2;

        // write the tuple offset in slot (2 bytes)
        let offset_bytes = offset.to_le_bytes();
        self.data[slot_start..tuple_offset_start].clone_from_slice(&offset_bytes);

        // write the tuple size in slot (2 bytes)
        let size_bytes = (value_size as u16).to_le_bytes();
        self.data[tuple_offset_start..slot_end].clone_from_slice(&size_bytes);

        // update the free space offset in page header
        let old_fs_offet = Self::combine_u8s_to_u16_le(&self.data, FS_OFFSET_0) as usize; // get old fs offset
        let new_free_space_offset = (old_fs_offet + value_size) as u16; // Move the free space down by the size of the inserted value
        let free_space_offset_bytes = new_free_space_offset.to_le_bytes();
        self.data[FS_OFFSET_0..FS_OFFSET_1 + 1].copy_from_slice(&free_space_offset_bytes);

        // update the free space size in page header
        let current_free_space_size = u16::from_le_bytes(self.data[FS_SIZE_0..FS_SIZE_1 + 1].try_into().unwrap()) as usize;
        let new_free_space_size = (current_free_space_size - value_size - SLOT_SIZE as usize) as u16;
        let free_space_size_bytes = new_free_space_size.to_le_bytes();
        self.data[FS_SIZE_0..FS_SIZE_1 + 1].copy_from_slice(&free_space_size_bytes);
    }

    // finds the id of a free slot
    fn find_free_slot(&self) -> Option<SlotId> {
        let num_slots = self.get_num_slots();

        if num_slots == 0 {
            println!("number of slots is 0, returning None");
            return None
        }

        for slot_id in 0..num_slots {
            let slot_offset = self.get_slot_offset(slot_id as u16);
            if slot_offset == INVALID_OFFSET {
                return Some(slot_id as u16); // Found a free slot
            }
        }
        None // No free slots found
    }

    // Helper function to get the offset for a given slot_id from the header
    fn get_slot_offset(&self, slot_id: SlotId) -> Offset {
        //calculate where the slot's metadata is stored in the header
        let start = FIXED_HEADER_SIZE + (slot_id * SLOT_SIZE) as usize;
        let end = start + 2; // 2 bytes for the offset
        u16::from_le_bytes(self.data[start..end].try_into().unwrap())
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        
        //check if the slot_id is valid
        let num_slots = self.get_num_slots();
        if slot_id >= num_slots as u16{
            return None;
        }

        //get tuple's offset and size from the header
        let (offset, size) = self.get_slot_metadata(slot_id)?;

        //if the offset is 0, the slot is free
        if offset == INVALID_OFFSET {
            return None;
        }

        //get the data bytes for the tuple
        let start = offset as usize;
        let end = start + size as usize;
        let tuple_bytes = self.data[start..end].to_vec();

        //return the extracted tuple bytes
        Some(tuple_bytes)
    }

    //get the size and offset for a tuple with a given slot ID
    fn get_slot_metadata(&self, slot_id: SlotId) -> Option<(Offset, usize)> {
        
        let start = FIXED_HEADER_SIZE + (slot_id * SLOT_SIZE) as usize;
        let offset = u16::from_le_bytes(self.data[start..start + 2].try_into().unwrap()); // get 2 bytes for tuple ofset
        let size = u16::from_le_bytes(self.data[start + 2..start + 4].try_into().unwrap()) as usize; //get 2 bytes for tuple size

        Some((offset, size))
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {

        //get the offset and size of the tuple to be deleted
        let (offset, size) = self.get_slot_metadata(slot_id)?;

        //mark the slot as free
        self.mark_slot_as_free(slot_id);

        //shift tuples left of deleted one to the right to compact the space
        self.compact_free_space(offset as usize, size as usize);

        //update offsets of the tuples in the header to their new values
        self.update_slots_after_deletion(offset as usize, size as usize);

        //add the freed space back to the total
        self.reclaim_free_space_header(size as usize);

        Some(())
    }

    // Compact the tuple space after deleting the tuple at the given offset.
    fn compact_free_space(&mut self, deleted_offset: usize, deleted_size: usize) {

        let fs_offset = Self::combine_u8s_to_u16_le(&self.data, FS_OFFSET_0) as usize;

        // Shift tuples rightward to fill the gap.
        let free_space_start = PAGE_SIZE - 1 - fs_offset;

        self.data[(free_space_start + 1)..(deleted_offset + deleted_size)].rotate_right(deleted_size);
    }

    // marks a given slot as free so it can be resused later
    fn mark_slot_as_free(&mut self, slot_id: SlotId) {
        let header_start = FIXED_HEADER_SIZE + (slot_id * SLOT_SIZE) as usize; //find slot in header
        self.data[header_start..header_start + 2].clone_from_slice(&INVALID_OFFSET.to_le_bytes()); //set offset to 0 to mark it as free
    }

    //adjust the offsets for all the tuples shifted to the right after a deletion
    fn update_slots_after_deletion(&mut self, deleted_offset: usize, deleted_size: usize) { //problem here!
        
        //iterate over the header and update the offsets of tuples that were shifted
        for slot_id in 0..self.get_num_slots() {
            let (offset, size) = self.get_slot_metadata(slot_id as u16).unwrap();

            // Only update the header if the tuple starts after the deleted tuple
            if (offset as usize) < deleted_offset && offset != INVALID_OFFSET {
                
                let new_offset = offset as usize + deleted_size;

                //update only the tuple's offset
                self.update_tuple_offset(slot_id as u16, new_offset as Offset);
            }
        }
    }

    //update the offset field in the slot for a given tuple 
    fn update_tuple_offset(&mut self, slot_id: SlotId, offset: Offset) {
        
        // calculate where the slot's metadata should be stored in the header
        let slot_start = FIXED_HEADER_SIZE + (slot_id * SLOT_SIZE) as usize;

        // Write the offset (2 bytes)
        let offset_bytes = offset.to_le_bytes();
        self.data[slot_start..slot_start + 2].clone_from_slice(&offset_bytes);
    }

    //add free space to the total and move offset when a tuple is deleted
    fn reclaim_free_space_header(&mut self, freed_size: usize) {

        // Free space offset + size of deleted item
        let free_space_offset = Self::combine_u8s_to_u16_le(&self.data, FS_OFFSET_0) as usize;

        // update the free space offset
        let free_space_offset_updated = (free_space_offset - freed_size) as u16;
        let free_space_offset_bytes = free_space_offset_updated.to_le_bytes();
        self.data[FS_OFFSET_0..FS_OFFSET_1 + 1].copy_from_slice(&free_space_offset_bytes);

        // update the free space size (increase it by the size of the deleted tuple)
        let current_free_space_size = u16::from_le_bytes(self.data[FS_SIZE_0..FS_SIZE_1 + 1].try_into().unwrap()) as usize;
        let new_free_space_size = (current_free_space_size + freed_size) as u16;
        let free_space_size_bytes = new_free_space_size.to_le_bytes();
        self.data[FS_SIZE_0..FS_SIZE_1 + 1].copy_from_slice(&free_space_size_bytes);
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests.
    #[allow(dead_code)]
    fn get_header_size(&self) -> usize {
        let num_slots = self.get_num_slots(); // get number of slots from data bytes in array

        return FIXED_HEADER_SIZE + (SLOT_SIZE as usize * num_slots);
    }

    //takes two contiguous indices in the data array and makes into a single u16
    fn combine_u8s_to_u16_le(arr: &[u8], idx: usize) -> u16 {
        u16::from_le_bytes([arr[idx], arr[idx + 1]])
    }

    /// A utility function to determine the total current free space in the page.
    /// This should account for the header space used and space that could be reclaimed if needed.
    /// Will be used by tests.
    #[allow(dead_code)]
    fn get_free_space(&self) -> usize {
        return Self::combine_u8s_to_u16_le(&self.data, FS_SIZE_0) as usize; // gets the free space fields from data array
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
pub struct HeapPageIntoIter {
    page: Page,
    // todo!("Add any fields you need here")
}

/// The implementation of the (consuming) page iterator.
/// This should return the values in slotId order (ascending)
impl Iterator for HeapPageIntoIter {
    // Each item returned by the iterator is the bytes for the value and the slot id.
    type Item = (Vec<u8>, SlotId);

    fn next(&mut self) -> Option<Self::Item> {
        todo!("Your code here")
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = (Vec<u8>, SlotId);
    type IntoIter = HeapPageIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        todo!("Your code here")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;
    use rand::Rng;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_sizes_header_free_space() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());

        assert_eq!(PAGE_SIZE - p.get_header_size(), p.get_free_space());
    }

    #[test]
    fn hs_page_debug_insert() {
        init();
        let mut p = Page::new(0);
        let n = 20;
        let size = 20;
        let vals = get_ascending_vec_of_byte_vec_02x(n, size, size);
        for x in &vals {
            p.add_value(x);
        }
        assert_eq!(
            p.get_free_space(),
            PAGE_SIZE - p.get_header_size() - n * size
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(5);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_free_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_free_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        // Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        // Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes); //problem!
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.to_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let mut p2 = p.clone();
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.to_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_serialization_is_deterministic() {
        init();

        // Create a page and serialize it
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        // Reconstruct the page
        let p1 = Page::from_bytes(*p0_bytes);
        let p1_bytes = p1.to_bytes();

        // Enforce that the two pages serialize determinestically
        assert_eq!(p0_bytes, p1_bytes);
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = *p.to_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes3.clone(), 2)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x.0);
        }

        let p = Page::from_bytes(page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = *p.to_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(Some((tuple_bytes.clone(), 4)), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_same_size() {
        init();
        let size = 800;
        let values = get_ascending_vec_of_byte_vec_02x(6, size, size);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0])); //not enough space for this one (correct)
        assert_eq!(Some(()), p.delete_value(1)); //problema!
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5])); // problem with reusing free slot (gets slot ID properly)
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_larger_size() {
        init();
        let size = 500;
        let values = get_ascending_vec_of_byte_vec_02x(8, size, size);
        let larger_val = get_random_byte_vec(size * 2 - 20);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(Some(5), p.add_value(&values[5]));
        assert_eq!(Some(6), p.add_value(&values[6]));
        assert_eq!(Some(7), p.add_value(&values[7]));
        assert_eq!(values[5], p.get_value(5).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(()), p.delete_value(6));
        assert_eq!(None, p.get_value(6));
        assert_eq!(Some(1), p.add_value(&larger_val));
        assert_eq!(larger_val, p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_smaller_size() {
        init();
        let size = 800;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size / 4),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_multi_ser() {
        init();
        let size = 500;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        let mut p2 = p.clone();
        assert_eq!(values[0], p2.get_value(0).unwrap());
        assert_eq!(values[1], p2.get_value(1).unwrap());
        assert_eq!(values[2], p2.get_value(2).unwrap());
        assert_eq!(Some(3), p2.add_value(&values[3]));
        assert_eq!(Some(4), p2.add_value(&values[4]));

        let mut p3 = p2.clone();
        assert_eq!(values[0], p3.get_value(0).unwrap());
        assert_eq!(values[1], p3.get_value(1).unwrap());
        assert_eq!(values[2], p3.get_value(2).unwrap());
        assert_eq!(values[3], p3.get_value(3).unwrap());
        assert_eq!(values[4], p3.get_value(4).unwrap());
        assert_eq!(Some(5), p3.add_value(&values[5]));
        assert_eq!(Some(6), p3.add_value(&values[6]));
        assert_eq!(Some(7), p3.add_value(&values[7]));
        assert_eq!(None, p3.add_value(&values[0]));

        let p4 = p3.clone();
        assert_eq!(values[0], p4.get_value(0).unwrap());
        assert_eq!(values[1], p4.get_value(1).unwrap());
        assert_eq!(values[2], p4.get_value(2).unwrap());
        assert_eq!(values[7], p4.get_value(7).unwrap());
    }

    #[test]
    pub fn hs_page_stress_test() {
        init();
        let mut p = Page::new(23);
        let mut original_vals: VecDeque<Vec<u8>> =
            VecDeque::from_iter(get_ascending_vec_of_byte_vec_02x(300, 20, 100));
        let mut stored_vals: Vec<Vec<u8>> = Vec::new();
        let mut stored_slots: Vec<SlotId> = Vec::new();
        let mut has_space = true;
        let mut rng = rand::thread_rng();
        // Load up page until full
        while has_space {
            let bytes = original_vals
                .pop_front()
                .expect("ran out of data -- shouldn't happen");
            let slot = p.add_value(&bytes);
            match slot {
                Some(slot_id) => {
                    stored_vals.push(bytes);
                    stored_slots.push(slot_id);
                }
                None => {
                    // No space for this record, we are done. go ahead and stop. add back value
                    original_vals.push_front(bytes);
                    has_space = false;
                }
            };
        }
        // let (check_vals, check_slots): (Vec<Vec<u8>>, Vec<SlotId>) = p.into_iter().map(|(a, b)| (a, b)).unzip();
        let p_clone = p.clone();
        let mut check_vals: Vec<Vec<u8>> = p_clone.into_iter().map(|(a, _)| a).collect();
        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
        trace!("\n==================\n PAGE LOADED - now going to delete to make room as needed \n =======================");
        // Delete and add remaining values until goes through all. Should result in a lot of random deletes and adds.
        while !original_vals.is_empty() {
            let bytes = original_vals.pop_front().unwrap();
            trace!("Adding new value (left:{}). Need to make space for new record (len:{}).\n - Stored_slots {:?}", original_vals.len(), &bytes.len(), stored_slots);
            let mut added = false;
            while !added {
                let try_slot = p.add_value(&bytes);
                match try_slot {
                    Some(new_slot) => {
                        stored_slots.push(new_slot);
                        stored_vals.push(bytes.clone());
                        let p_clone = p.clone();
                        check_vals = p_clone.into_iter().map(|(a, _)| a).collect();
                        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
                        trace!("Added new value ({}) {:?}", new_slot, stored_slots);
                        added = true;
                    }
                    None => {
                        //Delete a random value and try again
                        let random_idx = rng.gen_range(0..stored_slots.len());
                        trace!(
                            "Deleting a random val to see if that makes enough space {}",
                            stored_slots[random_idx]
                        );
                        let value_id_to_del = stored_slots.remove(random_idx);
                        stored_vals.remove(random_idx);
                        p.delete_value(value_id_to_del)
                            .expect("Error deleting slot_id");
                        trace!("Stored vals left {}", stored_slots.len());
                    }
                }
            }
        }
    }
}
