use super::OpIterator;
use crate::Managers;
use common::bytecode_expr::ByteCodeExpr;
use common::datatypes::f_decimal;
use common::{AggOp, CrustyError, Field, TableSchema, Tuple};
use sqlparser::keywords::PERCENTILE_CONT;
use std::cmp::{max, min};
use std::collections::HashMap;

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    // Static objects (No need to reset on close)
    managers: &'static Managers,

    // Parameters (No need to reset on close)
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Group by fields
    groupby_expr: Vec<ByteCodeExpr>,
    /// Aggregated fields.
    agg_expr: Vec<ByteCodeExpr>,
    /// Aggregation operations.
    ops: Vec<AggOp>,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
    /// If true, then the operator will be rewinded in the future.
    will_rewind: bool,
    
    // States (Need to reset on close)
    // todo!("Your code here")
    // Aggregation state
    agg_results: HashMap<Vec<Field>, Vec<Field>>, // Maps group keys to aggregate values
    count_map: HashMap<Vec<Field>, usize>,        // Tracks counts for avg operations
    result_tuples: Vec<Tuple>,                    // Stores finalized aggregation results
    result_index: usize,                          // Index to track position in result_tuples for iteration
    is_open: bool
}

impl Aggregate {
    pub fn new(
        managers: &'static Managers,
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
        schema: TableSchema,
        child: Box<dyn OpIterator>,
    ) -> Self {
        assert!(ops.len() == agg_expr.len());
        // todo!("Your code here")
        Self {
            managers,
            groupby_expr,
            agg_expr,
            ops,
            schema,
            child,
            will_rewind: false,
            agg_results: HashMap::new(),
            count_map: HashMap::new(),
            result_tuples: Vec::new(),
            result_index: 0,
            is_open: false
        }
    }

    //Self::merge_fields(*op, &field_val, &mut agg_values[i])?;
    fn merge_fields(op: AggOp, field_val: &Field, acc: &mut Field) -> Result<(), CrustyError> {
        match op {
            AggOp::Count => *acc = (acc.clone() + Field::Int(1))?,
            AggOp::Max => {
                let the_max;

                if acc.size() == 1 { //if acc is null, use field val
                    the_max = field_val.clone();
                } else if field_val.size() == 1 { //if field val is null, use acc
                    the_max = acc.clone();
                } else {  //if niether are null, figure out max of two
                    the_max = max(acc.clone(), field_val.clone());
                }
                *acc = the_max;
            }
            AggOp::Min => {
                let min = min(acc.clone(), field_val.clone());
                *acc = min;
            }
            AggOp::Sum => {
                *acc = (acc.clone() + field_val.clone())?;
            }
            AggOp::Avg => {

                *acc = (acc.clone() + field_val.clone())?; // This will be divided by the count later
            }
        }
        Ok(())
    }

    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) -> Result<(), CrustyError> {

        // Step 1: Calculate the group key by evaluating group-by expressions
        let group_key: Vec<Field> = self
            .groupby_expr
            .iter()
            .map(|expr| expr.eval(tuple))
            .collect();
    
        // Step 2: Initialize aggregation entry if the group key is new
        if !self.agg_results.contains_key(&group_key) {
            
            // Initialize aggregation vector with default values (e.g., 0 for Sum, Min, etc.)
            let initial_agg_values = self
                .ops
                .iter()
                .map(|op| match op {
                    AggOp::Count => Field::Int(0),
                    AggOp::Sum | AggOp::Avg => Field::Int(0), // Use 0 initially for Sum and Avg
                    AggOp::Min => Field::Null,       // Initialize Min to max possible value
                    AggOp::Max => Field::Null,       // Initialize Max to min possible value
                })
                .collect();
    
            self.agg_results.insert(group_key.clone(), initial_agg_values); //agg_results holds default values for this key now
            self.count_map.insert(group_key.clone(), 0);
        }
    
        // Step 3: For each aggregation operation, update the corresponding field in agg_results
        let agg_values = self.agg_results.get_mut(&group_key).unwrap(); //problem originates around here! agg_values is wrong

        for (i, op) in self.ops.iter().enumerate() {

            let field_val = self.agg_expr[i].eval(tuple);

            // Merge field value into aggregate using the specified operation
            Self::merge_fields(*op, &field_val, &mut agg_values[i])?;
        }
    
        // Step 4: Update count for Avg operations
        if self.ops.contains(&AggOp::Avg) {
            *self.count_map.get_mut(&group_key).unwrap() += 1;
        }
    
        Ok(())
    }
    


}

impl OpIterator for Aggregate {
    fn configure(&mut self, will_rewind: bool) {
        self.will_rewind = will_rewind;
        self.child.configure(false); // child of a aggregate will never be rewinded
                                     // because aggregate will buffer all the tuples from the child
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        //ppen the child operator
        self.child.open()?;
    
        // get all tuples from child and sort them into groups
        while let Some(tuple) = self.child.next()? {
            self.merge_tuple_into_group(&tuple)?;
        }
    
        // Finalize the aggregated results
        self.result_tuples.clear();
        for (group_key, agg_values) in &self.agg_results {
            let mut final_tuple = group_key.clone(); // Start with group-by fields
    
            for (i, field) in agg_values.iter().enumerate() {
                // Finalize average calculation if required
                let final_field = if self.ops[i] == AggOp::Avg { //final field is incorrect!! could be a string!!

                    let count = *self.count_map.get(group_key).unwrap_or(&1) as i64;
                    
                    if let Field::Int(sum) = field {
                        Field::Decimal((sum * 10000 / count), 4) //make 10,000 into a constant
                    } else {
                        return Err(CrustyError::CrustyError("Expected Int field for Avg".to_string()));
                    }
                
                } else {
                    field.clone()
                };
                final_tuple.push(final_field);
            }

            //final tuple is vec of fields
            //we need to turn this into a tuple

            //Tuple can be created from a vec of Fields

            self.result_tuples.push(Tuple::new(final_tuple));
        }
    
        // Reset result index for iteration
        self.result_index = 0;
        self.is_open = true;
    
        Ok(())
    }
    

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {

        if !self.is_open {
            panic!("Cannot call `next` before `open`.");
        }
        
        // Check if there are more tuples to return
        if self.result_index < self.result_tuples.len() {
            // Get the next tuple and increment the index
            let next_tuple = self.result_tuples[self.result_index].clone();
            self.result_index += 1;

            Ok(Some(next_tuple)) //next gives a vector of tuples
        } else {
            // No more tuples to return
            Ok(None)
        }
        //todo
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        
        // Close the child operator
        self.child.close()?;
    
        // Clear aggregation state
        self.agg_results.clear();
        self.count_map.clear();
        self.result_tuples.clear();
        self.result_index = 0;

        self.is_open = false;
    
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.is_open {
            panic!("Cannot call `rewind` before `open`.");
        }
    
        // Logic for rewinding
        self.result_index = 0;
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
    use crate::testutil::{execute_iter, new_test_managers, TestTuples};
    use common::{
        bytecode_expr::colidx_expr,
        datatypes::{f_int, f_str},
    };

    fn get_iter(
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
    ) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let managers = new_test_managers();
        let dummy_schema = TableSchema::new(vec![]);
        let mut iter = Box::new(Aggregate::new(
            managers,
            groupby_expr,
            agg_expr,
            ops,
            dummy_schema,
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
        ));
        iter.configure(false);
        iter
    }

    fn run_aggregate(
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
    ) -> Vec<Tuple> {
        let mut iter = get_iter(groupby_expr, agg_expr, ops);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod aggregation_test {
        use super::*;

        #[test]
        fn test_empty_group() {
            let group_by = vec![];
            let agg = vec![colidx_expr(0), colidx_expr(1), colidx_expr(2)];
            let ops = vec![AggOp::Count, AggOp::Max, AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            assert_eq!(t.len(), 1);
            assert_eq!(t[0], Tuple::new(vec![f_int(6), f_int(2), f_decimal(4.0)]));
        }

        #[test]
        fn test_empty_aggregation() {
            let group_by = vec![colidx_expr(2)];
            let agg = vec![];
            let ops = vec![];
            let t = run_aggregate(group_by, agg, ops);
            assert_eq!(t.len(), 3);
            assert_eq!(t[0], Tuple::new(vec![f_int(3)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(4)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(5)]));
        }

        #[test]
        fn test_count() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Count];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 2
            // 1 4 1
            // 2 4 1
            // 2 5 2
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_int(2)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_int(1)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_int(1)]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_int(2)]));
        }

        #[test]
        fn test_sum() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Sum];
            let tuples = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 3
            // 1 4 3
            // 2 4 4
            // 2 5 11
            assert_eq!(tuples.len(), 4);
            assert_eq!(tuples[0], Tuple::new(vec![f_int(1), f_int(3), f_int(3)]));
            assert_eq!(tuples[1], Tuple::new(vec![f_int(1), f_int(4), f_int(3)]));
            assert_eq!(tuples[2], Tuple::new(vec![f_int(2), f_int(4), f_int(4)]));
            assert_eq!(tuples[3], Tuple::new(vec![f_int(2), f_int(5), f_int(11)]));
        }

        #[test]
        fn test_max() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Max];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 G
            // 1 4 A
            // 2 4 G
            // 2 5 G
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_str("G")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_str("A")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_str("G")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_str("G")]));
        }

        #[test]
        fn test_min() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Min];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 E
            // 1 4 A
            // 2 4 G
            // 2 5 G
            assert!(t.len() == 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_str("E")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_str("A")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_str("G")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_str("G")]));
        }

        #[test]
        fn test_avg() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 1.5
            // 1 4 3.0
            // 2 4 4.0
            // 2 5 5.5
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_decimal(1.5)])); //fails
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_decimal(3.0)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_decimal(4.0)]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_decimal(5.5)]));
        }

        #[test]
        fn test_multi_column_aggregation() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(3)];
            let agg = vec![colidx_expr(0), colidx_expr(1), colidx_expr(2)];
            let ops = vec![AggOp::Count, AggOp::Max, AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // A 1 1 4.0
            // E 1 1 3.0
            // G 4 2 4.25
            assert_eq!(t.len(), 3);
            assert_eq!(
                t[0],
                Tuple::new(vec![f_str("A"), f_int(1), f_int(1), f_decimal(4.0)])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![f_str("E"), f_int(1), f_int(1), f_decimal(3.0)])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![f_str("G"), f_int(4), f_int(2), f_decimal(4.25)])
            );
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let group_by = vec![];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Avg];
            let _ = run_aggregate(group_by, agg, ops);
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter(vec![colidx_expr(2)], vec![colidx_expr(0)], vec![AggOp::Max]);
            iter.configure(true); // if we will rewind in the future, then we set will_rewind to true
            let t_before = execute_iter(&mut *iter, true).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, true).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
