//! Query utilities: filter evaluation and sort comparison for SchemaValues.
//!
//! This module provides two main functions:
//! - [`evaluate_filter`]: checks whether a `(lhs, op, rhs)` expression is true,
//!   with type-aware numeric promotion and IEEE 754 total ordering for floats.
//! - [`compare_for_sort`]: infallible sort ordering with nulls-last convention
//!   and cross-type ordering by a fixed kind ordinal.

use crate::engine::error::{EngineError, EngineResult};
use crate::engine::types::FilterOp;
use crate::schema_value::{SchemaValue, SchemaValueKind};
use std::cmp::Ordering;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compare two SchemaValues using the given filter operator.
///
/// Returns `true` if `(lhs OP rhs)` evaluates to true.
///
/// # Errors
///
/// Returns [`EngineError::UnsupportedQueryOperator`] when the combination of
/// types and operator is not supported — for example, comparing a `Bool` with
/// a greater-than operator, or comparing values of incompatible types.
///
/// # Supported comparisons
///
/// | Type(s)              | `Eq` / `Neq` | `Gt` / `Gte` / `Lt` / `Lte` |
/// |----------------------|--------------|-----------------------------|
/// | Any matching types   | Structural equality | Type-specific ordering     |
/// | Int32 ↔ Int64        | ✗            | Promoted to `i64`           |
/// | Int32/Int64 ↔ Float  | ✗            | Promoted to `f64`           |
/// | Bool, Object, Array, Json, Null | ✓ | Error                     |
pub fn evaluate_filter(lhs: &SchemaValue, op: FilterOp, rhs: &SchemaValue) -> EngineResult<bool> {
    match op {
        FilterOp::Eq => Ok(lhs == rhs),
        FilterOp::Neq => Ok(lhs != rhs),
        FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => {
            evaluate_inequality(lhs, op, rhs)
        }
    }
}

/// Compare two SchemaValues for sort ordering.
///
/// Returns [`Ordering::Less`], [`Ordering::Equal`], or [`Ordering::Greater`].
///
/// # Convention
///
/// - **Null sorts last**: a `Null` value is always `Greater` than any non-null
///   value, and equal to another `Null`.
/// - **Cross-type ordering**: when the kinds differ, the ordering follows the
///   kind ordinal:
///   `Null < Bool < Int32 < Int64 < Float < String < Bytes < Object < Array < Json`.
/// - **Same-type ordering**: delegated to the natural ordering for the type
///   (e.g. `i32::cmp`, `f64::total_cmp`, `str::cmp`).
/// - **Float**: always uses [`f64::total_cmp`] for a strict IEEE 754 total
///   order (NaN is already rejected by the codec, but [`debug_assert!`] guards
///   are present).
pub fn compare_for_sort(lhs: &SchemaValue, rhs: &SchemaValue) -> Ordering {
    // Null-sort-last: any Null is Greater than a non-Null.
    match (lhs, rhs) {
        (SchemaValue::Null, SchemaValue::Null) => return Ordering::Equal,
        (SchemaValue::Null, _) => return Ordering::Greater,
        (_, SchemaValue::Null) => return Ordering::Less,
        _ => {}
    }

    // Cross-type ordering by kind ordinal.
    let lhs_kind = lhs.kind();
    let rhs_kind = rhs.kind();
    if lhs_kind != rhs_kind {
        return schema_value_kind_ordinal(lhs_kind).cmp(&schema_value_kind_ordinal(rhs_kind));
    }

    // Same-type comparison.
    match (lhs, rhs) {
        (SchemaValue::Bool(a), SchemaValue::Bool(b)) => a.cmp(b),
        (SchemaValue::Int32(a), SchemaValue::Int32(b)) => a.cmp(b),
        (SchemaValue::Int64(a), SchemaValue::Int64(b)) => a.cmp(b),
        (SchemaValue::Float(a), SchemaValue::Float(b)) => {
            debug_assert!(a.is_finite());
            debug_assert!(b.is_finite());
            a.total_cmp(b)
        }
        (SchemaValue::String(a), SchemaValue::String(b)) => a.cmp(b),
        (SchemaValue::Bytes(a), SchemaValue::Bytes(b)) => a.cmp(b),
        (SchemaValue::Object(a), SchemaValue::Object(b)) => {
            a.len()
                .cmp(&b.len())
                .then_with(|| {
                    for (k, v) in a.iter() {
                        match b.get(k) {
                            Some(bv) => {
                                let c = compare_for_sort(v, bv);
                                if c != Ordering::Equal {
                                    return c;
                                }
                            }
                            None => return Ordering::Greater,
                        }
                    }
                    Ordering::Equal
                })
        }
        (SchemaValue::Array(a), SchemaValue::Array(b)) => {
            a.len()
                .cmp(&b.len())
                .then_with(|| {
                    for (va, vb) in a.iter().zip(b.iter()) {
                        let c = compare_for_sort(va, vb);
                        if c != Ordering::Equal {
                            return c;
                        }
                    }
                    Ordering::Equal
                })
        }
        (SchemaValue::Json(a), SchemaValue::Json(b)) => a.cmp(b),
        // Both Null was handled above — unreachable here.
        _ => unreachable!("all same-kind pairs covered"),
    }
}

// ---------------------------------------------------------------------------
// Helper: kind ordinal for cross-type sort ordering
// ---------------------------------------------------------------------------

/// Map a [`SchemaValueKind`] to a fixed ordinal for cross-type comparisons.
///
/// Ordering: `Null < Bool < Int32 < Int64 < Float < String < Bytes < Object < Array < Json`.
#[inline]
pub fn schema_value_kind_ordinal(kind: SchemaValueKind) -> u8 {
    match kind {
        SchemaValueKind::Null => 0,
        SchemaValueKind::Bool => 1,
        SchemaValueKind::Int32 => 2,
        SchemaValueKind::Int64 => 3,
        SchemaValueKind::Float => 4,
        SchemaValueKind::String => 5,
        SchemaValueKind::Bytes => 6,
        SchemaValueKind::Object => 7,
        SchemaValueKind::Array => 8,
        SchemaValueKind::Json => 9,
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn kind_name(kind: SchemaValueKind) -> &'static str {
    match kind {
        SchemaValueKind::Null => "Null",
        SchemaValueKind::Bool => "Bool",
        SchemaValueKind::Int32 => "Int32",
        SchemaValueKind::Int64 => "Int64",
        SchemaValueKind::Float => "Float",
        SchemaValueKind::String => "String",
        SchemaValueKind::Bytes => "Bytes",
        SchemaValueKind::Object => "Object",
        SchemaValueKind::Array => "Array",
        SchemaValueKind::Json => "Json",
    }
}

fn evaluate_inequality(lhs: &SchemaValue, op: FilterOp, rhs: &SchemaValue) -> EngineResult<bool> {
    let ordering = compare_for_inequality(lhs, rhs)?;
    match op {
        FilterOp::Gt => Ok(ordering == Ordering::Greater),
        FilterOp::Gte => Ok(ordering != Ordering::Less),
        FilterOp::Lt => Ok(ordering == Ordering::Less),
        FilterOp::Lte => Ok(ordering != Ordering::Greater),
        _ => unreachable!(),
    }
}

fn compare_for_inequality(lhs: &SchemaValue, rhs: &SchemaValue) -> EngineResult<Ordering> {
    // Fast path: same-variant comparisons.
    match (lhs, rhs) {
        // Same-type — use natural ordering.
        (SchemaValue::Int32(a), SchemaValue::Int32(b)) => return Ok(a.cmp(b)),
        (SchemaValue::Int64(a), SchemaValue::Int64(b)) => return Ok(a.cmp(b)),
        (SchemaValue::Float(a), SchemaValue::Float(b)) => {
            debug_assert!(a.is_finite());
            debug_assert!(b.is_finite());
            return Ok(a.total_cmp(b));
        }
        (SchemaValue::String(a), SchemaValue::String(b)) => return Ok(a.cmp(b)),
        (SchemaValue::Bytes(a), SchemaValue::Bytes(b)) => return Ok(a.cmp(b)),

        // Numeric promotion: Int32 ↔ Int64 → i64
        (SchemaValue::Int32(a), SchemaValue::Int64(b)) => return Ok((*a as i64).cmp(b)),
        (SchemaValue::Int64(a), SchemaValue::Int32(b)) => return Ok(a.cmp(&(*b as i64))),

        // Numeric promotion: Int32/Int64 ↔ Float → f64
        (SchemaValue::Int32(a), SchemaValue::Float(b)) => {
            debug_assert!(b.is_finite());
            return Ok((*a as f64).total_cmp(b));
        }
        (SchemaValue::Float(a), SchemaValue::Int32(b)) => {
            debug_assert!(a.is_finite());
            return Ok(a.total_cmp(&(*b as f64)));
        }
        (SchemaValue::Int64(a), SchemaValue::Float(b)) => {
            debug_assert!(b.is_finite());
            return Ok((*a as f64).total_cmp(b));
        }
        (SchemaValue::Float(a), SchemaValue::Int64(b)) => {
            debug_assert!(a.is_finite());
            return Ok(a.total_cmp(&(*b as f64)));
        }

        // Everything else is unsupported for inequality operators.
        _ => {}
    }

    let lhs_kind = lhs.kind();
    let rhs_kind = rhs.kind();
    let op_name = "inequality";
    // If one side is a non-comparable type, report that type.
    let field_type = if is_inequality_comparable(lhs_kind) {
        kind_name(rhs_kind)
    } else {
        kind_name(lhs_kind)
    };
    Err(EngineError::UnsupportedQueryOperator {
        field: String::new(),
        operator: op_name,
        field_type,
    })
}

fn is_inequality_comparable(kind: SchemaValueKind) -> bool {
    matches!(
        kind,
        SchemaValueKind::Int32
            | SchemaValueKind::Int64
            | SchemaValueKind::Float
            | SchemaValueKind::String
            | SchemaValueKind::Bytes
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_value::SchemaValue;

    // -----------------------------------------------------------------------
    // evaluate_filter: Eq / Neq
    // -----------------------------------------------------------------------

    #[test]
    fn eq_returns_true_for_equal_values() {
        assert!(evaluate_filter(&SchemaValue::Int32(42), FilterOp::Eq, &SchemaValue::Int32(42)).unwrap());
        assert!(evaluate_filter(&SchemaValue::Int64(99), FilterOp::Eq, &SchemaValue::Int64(99)).unwrap());
        assert!(evaluate_filter(&SchemaValue::String("hello".into()), FilterOp::Eq, &SchemaValue::String("hello".into())).unwrap());
        assert!(evaluate_filter(&SchemaValue::Bool(true), FilterOp::Eq, &SchemaValue::Bool(true)).unwrap());
        assert!(evaluate_filter(&SchemaValue::Null, FilterOp::Eq, &SchemaValue::Null).unwrap());
    }

    #[test]
    fn eq_returns_false_for_different_values() {
        assert!(!evaluate_filter(&SchemaValue::Int32(42), FilterOp::Eq, &SchemaValue::Int32(99)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Bool(true), FilterOp::Eq, &SchemaValue::Bool(false)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::String("abc".into()), FilterOp::Eq, &SchemaValue::String("xyz".into())).unwrap());
    }

    #[test]
    fn neq_returns_true_for_different_values() {
        assert!(evaluate_filter(&SchemaValue::Int32(42), FilterOp::Neq, &SchemaValue::Int32(99)).unwrap());
        assert!(evaluate_filter(&SchemaValue::Bool(true), FilterOp::Neq, &SchemaValue::Bool(false)).unwrap());
    }

    #[test]
    fn neq_returns_false_for_equal_values() {
        assert!(!evaluate_filter(&SchemaValue::Int32(42), FilterOp::Neq, &SchemaValue::Int32(42)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Null, FilterOp::Neq, &SchemaValue::Null).unwrap());
    }

    // -----------------------------------------------------------------------
    // evaluate_filter: inequality with same types
    // -----------------------------------------------------------------------

    #[test]
    fn int64_gt_works() {
        assert!(evaluate_filter(&SchemaValue::Int64(100), FilterOp::Gt, &SchemaValue::Int64(50)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Int64(50), FilterOp::Gt, &SchemaValue::Int64(100)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Int64(50), FilterOp::Gt, &SchemaValue::Int64(50)).unwrap());
    }

    #[test]
    fn int64_gte_at_boundary() {
        // Equal → Gte is true, Gt is false.
        assert!(evaluate_filter(&SchemaValue::Int64(50), FilterOp::Gte, &SchemaValue::Int64(50)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Int64(50), FilterOp::Gt, &SchemaValue::Int64(50)).unwrap());
        // Strictly greater → both work.
        assert!(evaluate_filter(&SchemaValue::Int64(100), FilterOp::Gte, &SchemaValue::Int64(50)).unwrap());
        assert!(evaluate_filter(&SchemaValue::Int64(100), FilterOp::Gt, &SchemaValue::Int64(50)).unwrap());
        // Strictly less → both false.
        assert!(!evaluate_filter(&SchemaValue::Int64(10), FilterOp::Gte, &SchemaValue::Int64(50)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Int64(10), FilterOp::Gt, &SchemaValue::Int64(50)).unwrap());
    }

    #[test]
    fn int32_lt_lte_work() {
        assert!(evaluate_filter(&SchemaValue::Int32(10), FilterOp::Lt, &SchemaValue::Int32(20)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Int32(20), FilterOp::Lt, &SchemaValue::Int32(10)).unwrap());
        assert!(evaluate_filter(&SchemaValue::Int32(10), FilterOp::Lte, &SchemaValue::Int32(10)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Int32(10), FilterOp::Lt, &SchemaValue::Int32(10)).unwrap());
    }

    #[test]
    fn string_gt_works() {
        assert!(evaluate_filter(&SchemaValue::String("banana".into()), FilterOp::Gt, &SchemaValue::String("apple".into())).unwrap());
        assert!(!evaluate_filter(&SchemaValue::String("apple".into()), FilterOp::Gt, &SchemaValue::String("banana".into())).unwrap());
        assert!(!evaluate_filter(&SchemaValue::String("apple".into()), FilterOp::Gt, &SchemaValue::String("apple".into())).unwrap());
    }

    #[test]
    fn string_lt_works() {
        assert!(evaluate_filter(&SchemaValue::String("apple".into()), FilterOp::Lt, &SchemaValue::String("banana".into())).unwrap());
        assert!(!evaluate_filter(&SchemaValue::String("banana".into()), FilterOp::Lt, &SchemaValue::String("apple".into())).unwrap());
    }

    #[test]
    fn float_comparison_uses_total_cmp() {
        // Normal floats.
        assert!(evaluate_filter(&SchemaValue::Float(3.14), FilterOp::Gt, &SchemaValue::Float(2.72)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Float(2.72), FilterOp::Gt, &SchemaValue::Float(3.14)).unwrap());
        // Equal.
        assert!(evaluate_filter(&SchemaValue::Float(1.0), FilterOp::Gte, &SchemaValue::Float(1.0)).unwrap());
        assert!(!evaluate_filter(&SchemaValue::Float(1.0), FilterOp::Gt, &SchemaValue::Float(1.0)).unwrap());
        // Negative zero vs positive zero — Eq uses f64 PartialEq (-0.0 == 0.0),
        // but Gt uses total_cmp (-0.0 < 0.0).
        let neg_zero = SchemaValue::Float(-0.0f64);
        let pos_zero = SchemaValue::Float(0.0f64);
        assert!(evaluate_filter(&neg_zero, FilterOp::Eq, &pos_zero).unwrap());
        assert!(!evaluate_filter(&neg_zero, FilterOp::Neq, &pos_zero).unwrap());
        assert!(evaluate_filter(&pos_zero, FilterOp::Gt, &neg_zero).unwrap());
    }

    #[test]
    fn bytes_inequality_works() {
        let a = SchemaValue::Bytes(vec![0x01, 0x02]);
        let b = SchemaValue::Bytes(vec![0x01, 0x03]);
        assert!(evaluate_filter(&a, FilterOp::Lt, &b).unwrap());
        assert!(evaluate_filter(&b, FilterOp::Gt, &a).unwrap());
        assert!(evaluate_filter(&a, FilterOp::Eq, &a).unwrap());
    }

    // -----------------------------------------------------------------------
    // evaluate_filter: numeric promotion
    // -----------------------------------------------------------------------

    #[test]
    fn int32_int64_promotion() {
        let i32_val = SchemaValue::Int32(42);
        let i64_val = SchemaValue::Int64(42);
        assert_eq!(evaluate_filter(&i32_val, FilterOp::Eq, &i64_val).unwrap(), false);
        assert_eq!(evaluate_filter(&i32_val, FilterOp::Neq, &i64_val).unwrap(), true);
        assert!(evaluate_filter(&i64_val, FilterOp::Gt, &SchemaValue::Int32(10)).unwrap());
        assert!(evaluate_filter(&SchemaValue::Int32(10), FilterOp::Lt, &i64_val).unwrap());
    }

    #[test]
    fn int32_float_promotion() {
        let i32 = SchemaValue::Int32(5);
        let f = SchemaValue::Float(5.0);
        assert_eq!(evaluate_filter(&i32, FilterOp::Eq, &f).unwrap(), false);
        assert!(evaluate_filter(&i32, FilterOp::Lt, &SchemaValue::Float(10.0)).unwrap());
        assert!(evaluate_filter(&SchemaValue::Float(1.0), FilterOp::Lt, &SchemaValue::Int32(5)).unwrap());
    }

    #[test]
    fn int64_float_promotion() {
        let i64 = SchemaValue::Int64(100);
        let f = SchemaValue::Float(99.9);
        assert!(evaluate_filter(&i64, FilterOp::Gt, &f).unwrap());
        assert!(evaluate_filter(&f, FilterOp::Lt, &i64).unwrap());
    }

    // -----------------------------------------------------------------------
    // evaluate_filter: type mismatch errors
    // -----------------------------------------------------------------------

    #[test]
    fn type_mismatch_returns_error() {
        let result = evaluate_filter(
            &SchemaValue::Int32(1),
            FilterOp::Gt,
            &SchemaValue::String("hello".into()),
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            EngineError::UnsupportedQueryOperator { .. } => {} // expected
            _ => panic!("expected UnsupportedQueryOperator"),
        }
    }

    #[test]
    fn bool_inequality_returns_error() {
        let result = evaluate_filter(
            &SchemaValue::Bool(true),
            FilterOp::Gt,
            &SchemaValue::Bool(false),
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            EngineError::UnsupportedQueryOperator { .. } => {}
            _ => panic!("expected UnsupportedQueryOperator"),
        }
    }

    #[test]
    fn null_inequality_returns_error() {
        let result = evaluate_filter(&SchemaValue::Null, FilterOp::Gt, &SchemaValue::Null);
        assert!(result.is_err());
        match result.unwrap_err() {
            EngineError::UnsupportedQueryOperator { .. } => {}
            _ => panic!("expected UnsupportedQueryOperator"),
        }
    }

    #[test]
    fn object_inequality_returns_error() {
        let obj = SchemaValue::object([("x".into(), SchemaValue::Int32(1))]);
        let result = evaluate_filter(&obj, FilterOp::Gt, &obj);
        assert!(result.is_err());
        match result.unwrap_err() {
            EngineError::UnsupportedQueryOperator { .. } => {}
            _ => panic!("expected UnsupportedQueryOperator"),
        }
    }

    // -----------------------------------------------------------------------
    // compare_for_sort
    // -----------------------------------------------------------------------

    #[test]
    fn sort_int64_ordering() {
        assert_eq!(compare_for_sort(&SchemaValue::Int64(10), &SchemaValue::Int64(20)), Ordering::Less);
        assert_eq!(compare_for_sort(&SchemaValue::Int64(20), &SchemaValue::Int64(10)), Ordering::Greater);
        assert_eq!(compare_for_sort(&SchemaValue::Int64(10), &SchemaValue::Int64(10)), Ordering::Equal);
    }

    #[test]
    fn sort_null_sorts_last() {
        // Null vs non-null
        assert_eq!(compare_for_sort(&SchemaValue::Null, &SchemaValue::Int32(5)), Ordering::Greater);
        assert_eq!(compare_for_sort(&SchemaValue::Int32(5), &SchemaValue::Null), Ordering::Less);
        // Null vs Null
        assert_eq!(compare_for_sort(&SchemaValue::Null, &SchemaValue::Null), Ordering::Equal);
    }

    #[test]
    fn sort_same_type_ordering() {
        // Bool: false < true
        assert_eq!(compare_for_sort(&SchemaValue::Bool(false), &SchemaValue::Bool(true)), Ordering::Less);
        assert_eq!(compare_for_sort(&SchemaValue::Bool(true), &SchemaValue::Bool(false)), Ordering::Greater);
        assert_eq!(compare_for_sort(&SchemaValue::Bool(true), &SchemaValue::Bool(true)), Ordering::Equal);

        // String lexicographic
        assert_eq!(
            compare_for_sort(&SchemaValue::String("a".into()), &SchemaValue::String("b".into())),
            Ordering::Less
        );

        // Bytes
        assert_eq!(
            compare_for_sort(&SchemaValue::Bytes(vec![0x01]), &SchemaValue::Bytes(vec![0x02])),
            Ordering::Less
        );

        // Float total_cmp
        assert_eq!(compare_for_sort(&SchemaValue::Float(1.0), &SchemaValue::Float(2.0)), Ordering::Less);
        assert_eq!(compare_for_sort(&SchemaValue::Float(-0.0), &SchemaValue::Float(0.0)), Ordering::Less);
    }

    #[test]
    fn sort_cross_type_ordering() {
        // Kind ordinal: Null < Bool < Int32 < Int64 < Float < String < Bytes < Object < Array < Json
        assert_eq!(compare_for_sort(&SchemaValue::Bool(true), &SchemaValue::Int32(0)), Ordering::Less);
        assert_eq!(compare_for_sort(&SchemaValue::Int32(0), &SchemaValue::Int64(0)), Ordering::Less);
        assert_eq!(compare_for_sort(&SchemaValue::Int64(0), &SchemaValue::Float(0.0)), Ordering::Less);
        assert_eq!(compare_for_sort(&SchemaValue::Float(0.0), &SchemaValue::String("".into())), Ordering::Less);
        assert_eq!(compare_for_sort(&SchemaValue::String("".into()), &SchemaValue::Bytes(vec![])), Ordering::Less);
        assert_eq!(compare_for_sort(&SchemaValue::Bytes(vec![]), &SchemaValue::object([])), Ordering::Less);
        assert_eq!(
            compare_for_sort(&SchemaValue::object([]), &SchemaValue::Array(vec![])),
            Ordering::Less
        );
        assert_eq!(
            compare_for_sort(&SchemaValue::Array(vec![]), &SchemaValue::Json(vec![])),
            Ordering::Less
        );
    }

    #[test]
    fn sort_object_comparison() {
        // Different field count.
        let small = SchemaValue::object([("a".into(), SchemaValue::Int32(1))]);
        let large = SchemaValue::object([
            ("a".into(), SchemaValue::Int32(1)),
            ("b".into(), SchemaValue::Int32(2)),
        ]);
        assert_eq!(compare_for_sort(&small, &large), Ordering::Less);
        assert_eq!(compare_for_sort(&large, &small), Ordering::Greater);

        // Same fields, same values → equal.
        let a1 = SchemaValue::object([("x".into(), SchemaValue::Int32(10))]);
        let a2 = SchemaValue::object([("x".into(), SchemaValue::Int32(10))]);
        assert_eq!(compare_for_sort(&a1, &a2), Ordering::Equal);

        // Same fields, different values.
        let b1 = SchemaValue::object([("x".into(), SchemaValue::Int32(5))]);
        let b2 = SchemaValue::object([("x".into(), SchemaValue::Int32(15))]);
        assert_eq!(compare_for_sort(&b1, &b2), Ordering::Less);
    }

    #[test]
    fn sort_array_comparison() {
        // Different length.
        let short = SchemaValue::Array(vec![SchemaValue::Int32(1)]);
        let long = SchemaValue::Array(vec![SchemaValue::Int32(1), SchemaValue::Int32(2)]);
        assert_eq!(compare_for_sort(&short, &long), Ordering::Less);

        // Same length, same elements.
        let a1 = SchemaValue::Array(vec![SchemaValue::Int32(1), SchemaValue::Int32(2)]);
        let a2 = SchemaValue::Array(vec![SchemaValue::Int32(1), SchemaValue::Int32(2)]);
        assert_eq!(compare_for_sort(&a1, &a2), Ordering::Equal);

        // Same length, different elements.
        let b1 = SchemaValue::Array(vec![SchemaValue::Int32(1), SchemaValue::Int32(2)]);
        let b2 = SchemaValue::Array(vec![SchemaValue::Int32(1), SchemaValue::Int32(99)]);
        assert_eq!(compare_for_sort(&b1, &b2), Ordering::Less);
    }

    #[test]
    fn sort_json_comparison() {
        let a = SchemaValue::Json(vec![0x01, 0x02, 0x03]);
        let b = SchemaValue::Json(vec![0x01, 0x02, 0x04]);
        assert_eq!(compare_for_sort(&a, &b), Ordering::Less);
        assert_eq!(compare_for_sort(&b, &a), Ordering::Greater);
        assert_eq!(compare_for_sort(&a, &a), Ordering::Equal);
    }

    /// Exercise every `SchemaValue` variant through `compare_for_sort` to
    /// guarantee it never panics.
    #[test]
    fn sort_no_panic_on_any_variant() {
        let values = vec![
            SchemaValue::Null,
            SchemaValue::Bool(true),
            SchemaValue::Int32(-1),
            SchemaValue::Int64(i64::MAX),
            SchemaValue::Float(3.14),
            SchemaValue::String("text".into()),
            SchemaValue::Bytes(vec![0xFF]),
            SchemaValue::object([("k".into(), SchemaValue::Int32(0))]),
            SchemaValue::Array(vec![SchemaValue::Null]),
            SchemaValue::Json(vec![0x00]),
        ];

        for a in &values {
            for b in &values {
                // Must not panic.
                let _ord = compare_for_sort(a, b);
            }
        }
    }

    // -----------------------------------------------------------------------
    // schema_value_kind_ordinal
    // -----------------------------------------------------------------------

    #[test]
    fn kind_ordinal_values() {
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Null), 0);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Bool), 1);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Int32), 2);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Int64), 3);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Float), 4);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::String), 5);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Bytes), 6);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Object), 7);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Array), 8);
        assert_eq!(schema_value_kind_ordinal(SchemaValueKind::Json), 9);
    }

    #[test]
    fn sort_cross_type_int32_vs_int64() {
        // Int32 should sort before Int64 (ordinal 2 < 3).
        assert_eq!(
            compare_for_sort(&SchemaValue::Int32(100), &SchemaValue::Int64(1)),
            Ordering::Less
        );
        assert_eq!(
            compare_for_sort(&SchemaValue::Int64(1), &SchemaValue::Int32(100)),
            Ordering::Greater
        );
    }
}
