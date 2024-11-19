use super::OpIterator;
use crate::Managers;

use common::bytecode_expr::ByteCodeExpr;
use common::{CrustyError, Field, TableSchema, Tuple};
use std::collections::HashMap;

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    // Static objects (No need to reset on close)
    managers: &'static Managers,

    // Parameters (No need to reset on close)
    schema: TableSchema,
    left_expr: ByteCodeExpr,
    right_expr: ByteCodeExpr,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    // States (Need to reset on close)
    // todo!("Your code here")
    hash_table: HashMap<Field, Vec<Tuple>>, // Maps left join keys to tuples
    right_buffer: Option<Tuple>,           // Current tuple from the right child
    output_buffer: Vec<Tuple>,             // Tuples to output from the join
}

impl HashEqJoin {
    /// NestedLoopJoin constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `left_expr` - ByteCodeExpr for the left field in join condition.
    /// * `right_expr` - ByteCodeExpr for the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        managers: &'static Managers,
        schema: TableSchema,
        left_expr: ByteCodeExpr,
        right_expr: ByteCodeExpr,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        HashEqJoin {
            left_expr,
            right_expr,
            left_child,
            right_child,
            hash_table: HashMap::new(),
            right_buffer: None,        
            output_buffer: Vec::new(), 
            schema,
            managers,                  
        }
    }
}

impl OpIterator for HashEqJoin {
    fn configure(&mut self, will_rewind: bool) {
        self.left_child.configure(false); // left child will never be rewound by HJ
        self.right_child.configure(will_rewind);
    }

    fn open(&mut self) -> Result<(), CrustyError> {

        //enable rewinding for both children
        self.left_child.configure(true);
        self.right_child.configure(true);

        //open the child operators
        self.left_child.open()?;
        self.right_child.open()?;
    
        //clear previous state in the has table
        self.hash_table.clear();
    
        //build the hash table from the left child
        while let Some(left_tuple) = self.left_child.next()? {
            
            //evaluate the join key for the left tuple
            let left_key = self.left_expr.eval(&left_tuple);
    
            //insert the tuple into the hash table under the corresponding key
            self.hash_table
                .entry(left_key)
                .or_insert_with(Vec::new)
                .push(left_tuple);
        }
    
        //reset the output buffer and right buffer
        self.output_buffer.clear();
        self.right_buffer = None;
    
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> { //problem here!
        //check if there are results in the output buffer
        if !self.output_buffer.is_empty() {
            return Ok(Some(self.output_buffer.remove(0)));
        }
    
        //for every tuple in the right child
        while let Some(right_tuple) = self.right_child.next()? {
            
            //store current right tuple in the buffer
            self.right_buffer = Some(right_tuple.clone());
    
            //evaluate join key for right tuple
            let right_key = self.right_expr.eval(&right_tuple);
    
            //probe hash table for matching tuples
            if let Some(left_tuples) = self.hash_table.get(&right_key) {
                
                //combine right tuple with all matching left tuples
                for left_tuple in left_tuples {
                    let mut joined_tuple = left_tuple.clone();
                    joined_tuple.field_vals.extend(right_tuple.field_vals.clone());
                    self.output_buffer.push(joined_tuple);
                }
    
                //return the first tuple from the output buffer
                if !self.output_buffer.is_empty() { //get one tuple from buffer and remove it
                    return Ok(Some(self.output_buffer.remove(0)));
                }
            }
        }
    
        //if no more tuples, return None
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        // Close both child operators
        self.left_child.close()?;
        self.right_child.close()?;
    
        // Clear internal state
        self.hash_table.clear();
        self.output_buffer.clear();
        self.right_buffer = None;
    
        Ok(())
    }
    
    fn rewind(&mut self) -> Result<(), CrustyError> {

        //rewind both child operators
        self.left_child.rewind()?;
        self.right_child.rewind()?;
    
        //clear and rebuild the hash table
        self.hash_table.clear();

        //reset buffers
        self.output_buffer.clear();
        self.right_buffer = None;
    
        Ok(())
    }
    
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::TupleIterator;
    use super::*;
    use crate::testutil::execute_iter;
    use crate::testutil::new_test_managers;
    use crate::testutil::TestTuples;
    use common::bytecode_expr::{ByteCodeExpr, ByteCodes};

    fn get_join_predicate() -> (ByteCodeExpr, ByteCodeExpr) {
        // Joining two tables each containing the following tuples:
        // 1 1 3 E
        // 2 1 3 G
        // 3 1 4 A
        // 4 2 4 G
        // 5 2 5 G
        // 6 2 5 G

        // left(col(0) + col(1)) OP right(col(2))
        let mut left = ByteCodeExpr::new();
        left.add_code(ByteCodes::PushField as usize);
        left.add_code(0);
        left.add_code(ByteCodes::PushField as usize);
        left.add_code(1);
        left.add_code(ByteCodes::Add as usize);

        let mut right = ByteCodeExpr::new();
        right.add_code(ByteCodes::PushField as usize);
        right.add_code(2);

        (left, right)
    }

    fn get_iter(left_expr: ByteCodeExpr, right_expr: ByteCodeExpr) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let managers = new_test_managers();
        let mut iter = Box::new(HashEqJoin::new(
            managers,
            setup.schema.clone(),
            left_expr,
            right_expr,
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
        ));
        iter.configure(false);
        iter
    }

    fn run_hash_eq_join(left_expr: ByteCodeExpr, right_expr: ByteCodeExpr) -> Vec<Tuple> {
        let mut iter = get_iter(left_expr, right_expr);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod hash_eq_join_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_empty_predicate_join() {
            let left_expr = ByteCodeExpr::new();
            let right_expr = ByteCodeExpr::new();
            let _ = run_hash_eq_join(left_expr, right_expr);
        }

        #[test]
        fn test_join() {
            // Joining two tables each containing the following tuples:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            // left(col(0) + col(1)) == right(col(2))

            // Output:
            // 2 1 3 G 1 1 3 E
            // 2 1 3 G 2 1 3 G
            // 3 1 4 A 3 1 4 A
            // 3 1 4 A 4 2 4 G
            let (left_expr, right_expr) = get_join_predicate();
            let t = run_hash_eq_join(left_expr, right_expr);
            assert_eq!(t.len(), 4);
            assert_eq!(
                t[0],
                Tuple::new(vec![
                    Field::Int(2),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("G".to_string()),
                    Field::Int(1),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("E".to_string()),
                ])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![
                    Field::Int(2),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("G".to_string()),
                    Field::Int(2),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("G".to_string()),
                ])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![
                    Field::Int(3),
                    Field::Int(1),
                    Field::Int(4),
                    Field::String("A".to_string()),
                    Field::Int(3),
                    Field::Int(1),
                    Field::Int(4),
                    Field::String("A".to_string()),
                ])
            );
            assert_eq!(
                t[3],
                Tuple::new(vec![
                    Field::Int(3),
                    Field::Int(1),
                    Field::Int(4),
                    Field::String("A".to_string()),
                    Field::Int(4),
                    Field::Int(2),
                    Field::Int(4),
                    Field::String("G".to_string()),
                ])
            );
        }
    }

    mod opiterator_test {
        use super::*;
        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() { //fails
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr); //hasheqjoin iterator
            iter.configure(true); 
            let t_before = execute_iter(&mut *iter, false).unwrap(); //open then kepp calling next to get all tuples
            iter.rewind().unwrap(); //rewind the iter
            let t_after = execute_iter(&mut *iter, false).unwrap(); //open and call next again
            assert_eq!(t_before, t_after);
        }
    }
}