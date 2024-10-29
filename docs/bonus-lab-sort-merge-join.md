_Due Date: Tuesday, December 10th, 2024 at 11:59 am (Noon)_

**No late days are allowed for the bonus lab. Any submissions after the deadline will not be graded.**

In this lab, you will implement the *sort-merge-join* operator in `queryexe/opiterator/sort_merge_join.rs` 

## Implementing the Sort-Merge-Join

This sort-merge join only considers equality join conditions. 
The `left_expr` and `right_expr` use the `Vec<(ByteCodeExpr, bool)>`, 
where each `ByteCodeExpr` represents a join key (e.g., `A.a`). 
This indicates this sort-merge join can support multiple join conditions 
(e.g., `A.a = B.a AND A.b = B.b`). 

However, our tests only cover one join condition, so you can restrict your solution 
to this case (i.e.,`left_expr.len()` == `right_expr.len()` == 1). 
Each join key is associated with a `bool` value to indicate the order to sort on the join key. 
`True` represents the ascending order while `False` represents the descending order. 
The orders for `left_expr` and `right_expr` should be the same. 
You can use `Field.cmp(Field)` to get the ordering of two fields.

As discussed in Lecture\#9, sort-merge join requires sorting the input data first. 
You could assume that we have enough memory to hold the input data from both subtrees 
and sort them in memory.

The merge-join phase is more challenging as you need to consider backtracking. In Lecture\#9, 
we have provided an algorithm for merge-joining two sorted data that supports backtracking. 
Note that it assumes both input data are ordered in an ascending order. You need to consider 
descending order as well, which should not be hard.

## Scoring and Requirements

### Testing

**Correctness**:
80% of your score on this lab is based on correctness. 
You can use the following commands to test your code:
`cargo test -p queryexe sort_merge_join` (6 tests), 

### Quality
10% of your score is based on code quality (following good coding conventions, comments, well-organized functions, etc). We will be looking for the following:

1. **Comments**: You should have comments for all new helper functions, constants and other identifiers that you add.
2. **Proper Types**: You should use suitable custom types. For example, you should use `SlotId` instead of `u16` when referring to a slot number. 
3. **Magic Numbers**: You should avoid magic numbers in your code. If you have a constant that is used in multiple places, you should define it as a constant at the top of the file.
4. **Descriptive Names**: Ensure that variables, functions, and constants have descriptive names that convey their purpose. Please don't use single-letter names or abbreviations unless they are widely recognized and contextually appropriate.

You could use `cargo fmt` to format your code in the right "style" and use 
`cargo clippy` to identify issues about your code, for either performance reasons or code quality. 
 
### Write Up
10% is based on your write up (`docs/bonus-lab-writeup.txt`). The write up should contain:
 -  A brief description of your solution, in particular what design decisions you took and why. This is only needed for part of your solutions that had some significant work (e.g. just returning a counter or a pass through function has no design decision).
- How long you roughly spent on the lab, and what would have liked/disliked on the lab.
- If you know some part of the lab is incomplete, write up what parts are not working, how close you think you are, and what part(s) you got stuck on.
