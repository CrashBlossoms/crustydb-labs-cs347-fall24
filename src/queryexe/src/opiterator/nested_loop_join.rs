use super::OpIterator;

use common::bytecode_expr::ByteCodeExpr;
use common::datatypes::compare_fields;
use common::{BooleanOp, CrustyError, TableSchema, Tuple};

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct NestedLoopJoin {
    // Parameters (No need to reset on close)
    schema: TableSchema,
    op: BooleanOp,
    left_expr: ByteCodeExpr,
    right_expr: ByteCodeExpr,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,

    // TODO: Add any other fields that you need to
    // maintain operator state here

    // State management fields
    current_left_tuple: Option<Tuple>,    // Holds the current tuple from the left child
    right_child_reset: bool,              // Flag to reset the right child for each new left tuple
    right_child_done: bool,               // Flag to indicate if right child is done for current left

}

impl NestedLoopJoin {
    /// NestedLoopJoin constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_expr` - ByteCodeExpr for the left field in join condition.
    /// * `right_expr` - ByteCodeExpr for the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: BooleanOp,
        left_expr: ByteCodeExpr,
        right_expr: ByteCodeExpr,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
        schema: TableSchema,
    ) -> Self {
        Self {
            schema,
            op,
            left_expr,
            right_expr,
            left_child,
            right_child,
            current_left_tuple: None,
            right_child_reset: true,
            right_child_done: false,
        }
        // todo!("Your code here")
    }
}

impl OpIterator for NestedLoopJoin {
    fn configure(&mut self, will_rewind: bool) {
        self.left_child.configure(will_rewind);
        self.right_child.configure(true); // right child will always be rewound by NLJ
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        // Reset state fields
        self.current_left_tuple = None;
        self.right_child_reset = true;
        self.right_child_done = false;

        // Open both left and right child iterators
        self.left_child.open()?;
        self.right_child.open()?;

        // Advance to the first tuple in the left relation
        self.current_left_tuple = self.left_child.next()?;

        Ok(())
        //todo
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {

        if self.current_left_tuple == None { //may not be correct
            panic!()
        }

        // Outer loop: Iterate through left tuples
        while let Some(ref left_tuple) = self.current_left_tuple {
            
            // Inner loop: Iterate through right tuples
            while let Some(right_tuple) = self.right_child.next()? {
                
                // Evaluate left and right expressions
                let left_value = self.left_expr.eval(left_tuple);
                let right_value = self.right_expr.eval(&right_tuple);

                // Apply the join condition using the specified operation
                if compare_fields(self.op, &left_value, &right_value) {

                    // Concatenate or join the tuples and return the result
                    let joined_tuple = left_tuple.merge(&right_tuple);
                    return Ok(Some(joined_tuple));
                }
            }

            // Reset the right iterator and advance to the next tuple in the left iterator
            self.right_child.rewind()?;
            self.current_left_tuple = self.left_child.next()?;
        }

        // No more tuples to join
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        // Close both child iterators
        self.left_child.close()?;
        self.right_child.close()?;

        // Reset internal state fields
        self.current_left_tuple = None;
        self.right_child_reset = true;
        self.right_child_done = false;

        Ok(())
        //todo
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        // Rewind both child iterators to start from the beginning
        self.left_child.rewind()?;
        self.right_child.rewind()?;
    
        // Reinitialize state
        self.current_left_tuple = self.left_child.next()?;
        self.right_child_reset = true;
        self.right_child_done = false;
    
        Ok(())
        //todo
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::TupleIterator;
    use super::*;
    use crate::testutil::execute_iter;
    use crate::testutil::TestTuples;
    use common::bytecode_expr::{ByteCodeExpr, ByteCodes};
    use common::Field;

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

    fn get_iter(
        op: BooleanOp,
        left_expr: ByteCodeExpr,
        right_expr: ByteCodeExpr,
    ) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let mut iter = Box::new(NestedLoopJoin::new(
            op,
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
            setup.schema.clone(),
        ));
        iter.configure(false);
        iter
    }

    fn run_nested_loop_join(
        op: BooleanOp,
        left_expr: ByteCodeExpr,
        right_expr: ByteCodeExpr,
    ) -> Vec<Tuple> {
        let mut iter = get_iter(op, left_expr, right_expr);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod nested_loop_join_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_empty_predicate_join() {
            let left_expr = ByteCodeExpr::new();
            let right_expr = ByteCodeExpr::new();
            let _ = run_nested_loop_join(BooleanOp::Eq, left_expr, right_expr);
        }

        #[test]
        fn test_eq_join() {
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
            let t = run_nested_loop_join(BooleanOp::Eq, left_expr, right_expr);
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
            let mut iter = get_iter(BooleanOp::Eq, left_expr, right_expr);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BooleanOp::Eq, left_expr, right_expr);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BooleanOp::Eq, left_expr, right_expr);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BooleanOp::Eq, left_expr, right_expr);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BooleanOp::Eq, left_expr, right_expr);
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
