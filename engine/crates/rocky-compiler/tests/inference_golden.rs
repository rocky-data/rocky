//! Golden corpus of inferred `(RockyType, nullable)` pairs (ADR-TYPES §8).
//!
//! Each row is a SQL expression over one fixed source table and the pair
//! Rocky infers for it, with no target warehouse configured. A change to
//! an expected pair is a public behaviour change: it needs a `Changed`
//! entry in `engine/CHANGELOG.md` that names the expression, the old pair
//! and the new pair.
//!
//! The rows cover the decimal algebra of ADR-TYPES §3: the arithmetic
//! operators and the branch-set unification of `COALESCE`, `CASE`,
//! `GREATEST` and `LEAST`. Numeric literals are left out on purpose: their
//! type is ADR-TYPES Open question G.

use std::collections::HashMap;

use rocky_compiler::typecheck::infer_select_types;
use rocky_compiler::types::{RockyType, TypedColumn};

fn dec(precision: u8, scale: u8) -> RockyType {
    RockyType::Decimal { precision, scale }
}

fn sources() -> HashMap<String, Vec<TypedColumn>> {
    let column = |name: &str, data_type: RockyType, nullable: bool| TypedColumn {
        name: name.to_string(),
        data_type,
        nullable,
    };
    HashMap::from([(
        "t".to_string(),
        vec![
            column("i", RockyType::Int32, false),
            column("j", RockyType::Int32, true),
            column("b", RockyType::Int64, false),
            column("d", dec(10, 2), false),
            column("e", dec(10, 0), false),
            column("f", dec(10, 10), true),
            column("g", dec(5, 2), false),
            column("w", dec(30, 0), false),
            column("z", dec(30, 30), false),
            column("x", RockyType::Float64, false),
            column("r", RockyType::Float32, false),
            column("s", RockyType::String, false),
            column("dt", RockyType::Date, false),
            column("u", RockyType::DecimalUnsized { scale: Some(2) }, true),
        ],
    )])
}

fn infer(expr: &str) -> (RockyType, bool) {
    let sql = format!("SELECT {expr} AS c FROM t");
    let columns = infer_select_types(&sql, &sources(), "m").unwrap();
    (columns[0].data_type.clone(), columns[0].nullable)
}

#[test]
fn inferred_types_match_the_golden_corpus() {
    let unsized_none = RockyType::DecimalUnsized { scale: None };
    let unsized_2 = RockyType::DecimalUnsized { scale: Some(2) };
    let corpus: Vec<(&str, RockyType, bool)> = vec![
        // Decimal arithmetic: one rule per operator.
        ("d + d", dec(11, 2), false),
        ("d - e", dec(13, 2), false),
        ("d * d", dec(20, 4), false),
        ("d / d", RockyType::Unknown, true),
        ("d % e", dec(10, 2), true),
        ("e + f", dec(21, 10), true),
        // An integer keeps its integer digits.
        ("i + d", dec(13, 2), false),
        ("b + d", dec(22, 2), false),
        ("i * g", dec(15, 2), false),
        // Above 38 digits: no type, never a clamped scale, and nullable
        // (ADR-TYPES §1): an overflow is NULL on Databricks with ANSI off.
        ("w + w", dec(31, 0), false),
        ("w * w", RockyType::Unknown, true),
        ("w + z", RockyType::Unknown, true),
        ("w - z", RockyType::Unknown, true),
        // Integer pairs.
        ("i + j", RockyType::Int64, true),
        ("i * i", RockyType::Int64, false),
        ("i / i", RockyType::Unknown, true),
        ("b % i", RockyType::Int64, true),
        // A float on either side.
        ("x * d", RockyType::Float64, false),
        ("r + i", RockyType::Float64, false),
        ("r * r", RockyType::Float64, false),
        ("x / i", RockyType::Float64, true),
        // A decimal of unknown precision stays unsized.
        ("u + d", unsized_none.clone(), true),
        ("u * i", unsized_none.clone(), true),
        ("u / d", RockyType::Unknown, true),
        ("SUM(d)", unsized_none.clone(), true),
        // Not a number.
        ("s + i", RockyType::Unknown, false),
        ("dt + i", RockyType::Unknown, false),
        // Unification: max integer digits, max scale, no carry digit.
        ("COALESCE(e, f)", dec(20, 10), false),
        ("COALESCE(f, e)", dec(20, 10), false),
        ("COALESCE(i, g)", dec(12, 2), false),
        ("COALESCE(i, b)", RockyType::Int64, false),
        ("COALESCE(d, d)", dec(10, 2), false),
        ("COALESCE(u, d)", unsized_2.clone(), false),
        ("COALESCE(u, i)", unsized_2.clone(), false),
        // A NULL literal adds only nullability.
        ("COALESCE(j, NULL)", RockyType::Int32, true),
        ("COALESCE(NULL, d)", dec(10, 2), false),
        ("CASE WHEN i > 0 THEN d ELSE NULL END", dec(10, 2), true),
        // Incompatible branches, or a branch Rocky cannot type.
        ("COALESCE(d, s)", RockyType::Unknown, false),
        ("COALESCE(d, x)", RockyType::Unknown, false),
        ("COALESCE(d, MYSTERY(d))", RockyType::Unknown, false),
        ("CASE WHEN i > 0 THEN e ELSE f END", dec(20, 10), true),
        ("CASE WHEN i > 0 THEN d END", dec(10, 2), true),
        ("GREATEST(e, f)", dec(20, 10), true),
        ("LEAST(i, d)", dec(12, 2), true),
        // A branch set above 38 digits: no type, and nullable, as above.
        ("COALESCE(w, z)", RockyType::Unknown, true),
        (
            "CASE WHEN i > 0 THEN w ELSE z END",
            RockyType::Unknown,
            true,
        ),
    ];
    let mut failures = Vec::new();
    for (expr, ty, nullable) in corpus {
        let got = infer(expr);
        if got != (ty.clone(), nullable) {
            failures.push(format!("{expr}: got {got:?}, want {:?}", (ty, nullable)));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// ADR-TYPES §3: branches are unified as a set, so the order of the
/// arguments cannot change the result. `w`, `z` and `g` need 60 digits
/// (`DECIMAL(30,0)` with `DECIMAL(30,30)`), so the set is `Unknown` in every
/// order. Main's pairwise fold takes `max(precision)` and `max(scale)`, so
/// it gives `DECIMAL(30,30)` in every order: independent of the order, but
/// wrong, because `DECIMAL(30,30)` has no integer digits and cannot hold
/// `w`. A fold that applies the cap and starts from `Unknown` would depend
/// on the order: it reaches `Unknown` after `w, z`, and `g` then replaces
/// it with `DECIMAL(5,2)`.
#[test]
fn branch_sets_do_not_depend_on_argument_order() {
    let sets: [([&str; 3], RockyType); 3] = [
        (["w", "z", "g"], RockyType::Unknown),
        (["e", "f", "i"], dec(20, 10)),
        (
            ["u", "d", "i"],
            RockyType::DecimalUnsized { scale: Some(2) },
        ),
    ];
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for (set, want) in sets {
        for order in orders {
            let [a, b, c] = order.map(|i| set[i]);
            for expr in [
                format!("COALESCE({a}, {b}, {c})"),
                format!("CASE WHEN i > 0 THEN {a} WHEN i > 1 THEN {b} ELSE {c} END"),
                format!("GREATEST({a}, {b}, {c})"),
                format!("LEAST({a}, {b}, {c})"),
            ] {
                assert_eq!(infer(&expr).0, want, "{expr}");
            }
        }
    }
}
