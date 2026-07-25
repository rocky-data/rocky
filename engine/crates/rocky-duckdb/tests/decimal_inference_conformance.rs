//! Live-execute cross-check for Rocky's decimal arithmetic inference (RD-010).
//!
//! Every rule in `rocky_ir::types::arithmetic_result_type` is derived as an
//! upper bound on the *exact mathematical result*, not copied from any engine's
//! formula. This suite executes each rule against a real DuckDB and checks the
//! relationship the design actually claims.
//!
//! # What this asserts, and what it deliberately does not
//!
//! The primary assertion is against the **exact mathematical bound**, which the
//! test recomputes from first principles rather than reading back from the
//! implementation:
//!
//! ```text
//! rocky is Unknown  OR  (int_digits(rocky) >= exact_int_digits
//!                        AND scale(rocky)  >= exact_scale)
//! ```
//!
//! Asserting equality with DuckDB would be wrong in *both* directions, and each
//! direction is measured here rather than assumed:
//!
//! - **Rocky is sometimes wider.** DuckDB types `DECIMAL(10,2) * DECIMAL(10,2)`
//!   as `DECIMAL(18,4)` — fourteen integer digits, where the exact product needs
//!   sixteen. Asserting equality would encode DuckDB's under-provisioning as
//!   Rocky's expectation.
//! - **Rocky is sometimes tighter.** DuckDB types `DECIMAL(10,2) % DECIMAL(10,0)`
//!   as `DECIMAL(12,2)`, but a remainder is bounded by *both* operands
//!   (`|a mod b| <= |a|` and `< |b|`), so eight integer digits suffice and
//!   DuckDB over-provisions. Rocky being narrower than an engine is only a
//!   defect if the engine was itself already tight — which is exactly the
//!   secondary assertion below.
//!
//! An over-wide inferred type produces at worst a false *reject* at a contract
//! gate, which is recoverable; a too-narrow one produces a false *permit*, which
//! is what RD-010 is about.
//!
//! # Coverage boundary, stated plainly
//!
//! **DuckDB only.** Databricks, Snowflake, BigQuery and Trino are not covered by
//! this suite and this PR does not claim they are — their decimal behaviour was
//! derived from documentation, not executed. Because Rocky's position is
//! "sound upper bound, not prediction", a mismatch on another engine would
//! inform the rules rather than invalidate them, but it would still be new
//! information. Running the equivalent probe on the Databricks sandbox is the
//! highest-value verification left.
//!
//! Emitted SQL needs no equivalent test: Rocky emits `CREATE TABLE … AS SELECT`
//! and no adapter maps `RockyType` back to a warehouse type string (verified by
//! grep across every adapter crate), so an inferred type can never reach DDL.

use rocky_duckdb::DuckDbConnector;
use rocky_ir::types::{DecimalOp, RockyType, arithmetic_result_type, decimal_parts_of};

/// Parse a DuckDB result-type string into `(integer_digits, scale)`.
///
/// Returns `None` for a type that is not exact-numeric — notably `DOUBLE`,
/// which is what DuckDB returns for decimal division.
fn duckdb_parts(type_name: &str) -> Option<(u32, u32)> {
    let upper = type_name.trim().to_uppercase();
    match upper.as_str() {
        "TINYINT" => return Some((3, 0)),
        "SMALLINT" => return Some((5, 0)),
        "INTEGER" | "INT" => return Some((10, 0)),
        "BIGINT" => return Some((19, 0)),
        "HUGEINT" => return Some((39, 0)),
        _ => {}
    }
    let params = upper
        .strip_prefix("DECIMAL(")
        .or_else(|| upper.strip_prefix("NUMERIC("))?
        .strip_suffix(')')?;
    let (precision, scale) = params.split_once(',')?;
    let precision: u32 = precision.trim().parse().ok()?;
    let scale: u32 = scale.trim().parse().ok()?;
    Some((precision.checked_sub(scale)?, scale))
}

/// The SQL literal for a decimal operand of the given shape, chosen so DuckDB
/// infers exactly `DECIMAL(precision, scale)`.
fn operand_sql(precision: u8, scale: u8) -> String {
    format!("CAST(1 AS DECIMAL({precision},{scale}))")
}

fn operand_sql_for(ty: &RockyType) -> String {
    match ty {
        RockyType::Int32 => "CAST(1 AS INTEGER)".to_string(),
        RockyType::Int64 => "CAST(1 AS BIGINT)".to_string(),
        RockyType::Decimal { precision, scale } => operand_sql(*precision, *scale),
        other => panic!("unsupported probe operand: {other}"),
    }
}

fn op_sql(op: DecimalOp) -> &'static str {
    match op {
        DecimalOp::Add => "+",
        DecimalOp::Subtract => "-",
        DecimalOp::Multiply => "*",
        DecimalOp::Divide => "/",
        DecimalOp::Modulo => "%",
    }
}

/// Ask DuckDB for the result type of `<a> <op> <b>` without evaluating any row.
fn duckdb_result_type(conn: &DuckDbConnector, a: &str, op: &str, b: &str) -> String {
    let sql = format!("SELECT typeof({a} {op} {b}) AS t");
    let result = conn
        .execute_sql(&sql)
        .unwrap_or_else(|e| panic!("DuckDB rejected probe `{sql}`: {e}"));
    let cell = &result.rows[0][0];
    cell.as_str()
        .unwrap_or_else(|| panic!("typeof() returned a non-string cell for `{sql}`: {cell}"))
        .to_string()
}

/// The exact bound on `a op b`, derived independently of the implementation so
/// this test is evidence rather than a restatement.
///
/// Returns `None` for division, whose exact quotient is generally
/// non-terminating and has no `(integer_digits, scale)` bound.
fn exact_bound(op: DecimalOp, (i1, s1): (u32, u32), (i2, s2): (u32, u32)) -> Option<(u32, u32)> {
    match op {
        // |a ± b| < 2·10^max(i1,i2) <= 10^(max+1); the exact sum has at most
        // max(s1,s2) fractional digits.
        DecimalOp::Add | DecimalOp::Subtract => Some((i1.max(i2) + 1, s1.max(s2))),
        // |a·b| < 10^i1 · 10^i2; the exact product has exactly s1+s2 of them.
        DecimalOp::Multiply => Some((i1 + i2, s1 + s2)),
        // a - b·trunc(a/b): bounded by |a| AND by |b|, so the tighter of the two
        // holds. Scale is at most max(s1,s2).
        DecimalOp::Modulo => Some((i1.min(i2), s1.max(s2))),
        DecimalOp::Divide => None,
    }
}

/// The core claim, in two parts:
///
/// 1. Rocky's inferred type is `Unknown` or holds the **exact** result.
/// 2. Where Rocky is narrower than DuckDB, DuckDB was over-provisioning — Rocky
///    is never narrower than an engine result that was itself tight.
#[test]
fn rocky_decimal_inference_bounds_the_exact_result() {
    let conn = DuckDbConnector::in_memory().expect("in-memory DuckDB");
    let dec = |precision: u8, scale: u8| RockyType::Decimal { precision, scale };

    let operands = [
        dec(10, 2),
        dec(10, 0),
        dec(10, 10),
        dec(5, 2),
        dec(1, 1),
        dec(19, 0),
        dec(38, 0),
        RockyType::Int32,
        RockyType::Int64,
    ];
    let ops = [
        DecimalOp::Add,
        DecimalOp::Subtract,
        DecimalOp::Multiply,
        DecimalOp::Divide,
        DecimalOp::Modulo,
    ];

    let mut compared = 0usize;
    let mut degraded = 0usize;
    let mut rocky_wider = 0usize;
    let mut rocky_tighter = 0usize;
    let mut family_diverged = 0usize;

    for a in &operands {
        for b in &operands {
            // At least one decimal operand — integer/integer pairs keep the
            // pre-existing behaviour and are out of scope (see the scope note on
            // `arithmetic_result_type`).
            if !a.is_decimal() && !b.is_decimal() {
                continue;
            }
            for op in ops {
                let rocky = arithmetic_result_type(op, a, b);
                if rocky == RockyType::Unknown {
                    degraded += 1;
                    continue; // conservative degradation is always allowed
                }

                let parts_a = decimal_parts_of(a).expect("well-formed probe operand");
                let parts_b = decimal_parts_of(b).expect("well-formed probe operand");
                let (need_int, need_scale) =
                    exact_bound(op, parts_a, parts_b).unwrap_or_else(|| {
                        panic!(
                            "{a} {op:?} {b}: Rocky inferred {rocky} for an operation with no bound"
                        )
                    });
                let (rocky_int, rocky_scale) = decimal_parts_of(&rocky).unwrap_or_else(|| {
                    panic!("{a} {op:?} {b}: Rocky produced non-exact-numeric {rocky}")
                });

                // (1) Soundness against the exact result — the claim that
                // actually matters.
                assert!(
                    rocky_int >= need_int && rocky_scale >= need_scale,
                    "{a} {op:?} {b}: Rocky inferred {rocky} ({rocky_int} int digits, \
                     scale {rocky_scale}) but the exact result needs {need_int} int \
                     digits and scale {need_scale}"
                );

                // (2) Direction of divergence from a real engine.
                let duck =
                    duckdb_result_type(&conn, &operand_sql_for(a), op_sql(op), &operand_sql_for(b));
                let Some((duck_int, duck_scale)) = duckdb_parts(&duck) else {
                    // DuckDB left the exact-numeric family — it returns DOUBLE
                    // for decimal division, and also for e.g.
                    // `DECIMAL(10,2) % DECIMAL(38,0)`, where its own formula has
                    // no wider decimal left to widen into.
                    //
                    // This is NOT a Rocky defect. Assertion (1) above already
                    // proved Rocky's decimal holds the exact result, so Rocky is
                    // both sound and strictly more precise than DuckDB here.
                    // Counted rather than asserted on, because which pairs land
                    // in this class is a DuckDB implementation detail.
                    family_diverged += 1;
                    compared += 1;
                    continue;
                };

                if rocky_int < duck_int || rocky_scale < duck_scale {
                    // Rocky narrower than DuckDB is only acceptable where DuckDB
                    // itself exceeded the exact bound. If DuckDB was tight and
                    // Rocky is narrower, Rocky is unsound — that is the failure
                    // this branch exists to catch.
                    assert!(
                        duck_int > need_int || duck_scale > need_scale,
                        "{a} {op:?} {b}: Rocky inferred {rocky} ({rocky_int} int digits, \
                         scale {rocky_scale}), narrower than DuckDB's {duck} ({duck_int} \
                         int digits, scale {duck_scale}), and DuckDB was already tight \
                         against the exact bound ({need_int} int digits, scale {need_scale})"
                    );
                    rocky_tighter += 1;
                } else if rocky_int > duck_int || rocky_scale > duck_scale {
                    rocky_wider += 1;
                }
                compared += 1;
            }
        }
    }

    assert!(
        compared >= 100,
        "expected a substantive comparison set, only compared {compared} pairs"
    );
    // Degradations are expected (all decimal division, plus every result past
    // precision 38) but must not be the whole suite.
    assert!(degraded > 0, "expected some conservative degradations");
    // Both divergence directions are expected and are the point of the design's
    // "upper bound, not prediction" position. If either becomes zero, DuckDB's
    // formulas have changed and the rationale needs rereading.
    assert!(
        rocky_wider > 0 && rocky_tighter > 0,
        "expected divergence in both directions (wider: {rocky_wider}, \
         tighter: {rocky_tighter}) — DuckDB's decimal formulas may have changed"
    );
    // DuckDB drops out of the exact-numeric family on some exact-numeric
    // operations where Rocky stays exact. Recorded so the fact is visible.
    assert!(
        family_diverged > 0,
        "expected at least one pair where DuckDB falls back to a float and \
         Rocky stays exact"
    );
}

/// The measured cases the design's rule table cites by name. These pin the
/// specific DuckDB answers the design was derived against, so a DuckDB upgrade
/// that changes them is caught rather than silently invalidating the rationale.
#[test]
fn duckdb_reference_results_match_the_design_table() {
    let conn = DuckDbConnector::in_memory().expect("in-memory DuckDB");
    let d = |p: u8, s: u8| operand_sql(p, s);

    // Integer promotion — this is the evidence for Int32 -> 10 integer digits
    // and Int64 -> 19, rather than Spark's LONG -> DECIMAL(20,0).
    assert_eq!(
        duckdb_result_type(&conn, "CAST(1 AS INTEGER)", "+", &d(10, 2)),
        "DECIMAL(13,2)",
        "11 integer digits = max(10, 8) + 1, so Int32 carries 10"
    );
    assert_eq!(
        duckdb_result_type(&conn, "CAST(1 AS BIGINT)", "+", &d(10, 2)),
        "DECIMAL(22,2)",
        "20 integer digits = max(19, 8) + 1, so Int64 carries 19"
    );

    // Add carries exactly one digit — Rocky matches here.
    assert_eq!(
        duckdb_result_type(&conn, &d(10, 2), "+", &d(10, 2)),
        "DECIMAL(11,2)"
    );
    // Modulo — Rocky matches here too.
    assert_eq!(
        duckdb_result_type(&conn, &d(10, 2), "%", &d(10, 2)),
        "DECIMAL(10,2)"
    );

    // Division leaves the exact-numeric family entirely, which is why Rocky
    // returns Unknown rather than picking any engine's rounding policy.
    assert_eq!(
        duckdb_result_type(&conn, &d(10, 2), "/", &d(10, 2)),
        "DOUBLE"
    );
    assert_eq!(
        arithmetic_result_type(
            DecimalOp::Divide,
            &RockyType::Decimal {
                precision: 10,
                scale: 2
            },
            &RockyType::Decimal {
                precision: 10,
                scale: 2
            },
        ),
        RockyType::Unknown
    );
}

/// Modulo is the case where Rocky is deliberately **tighter** than DuckDB, and
/// it is sound.
///
/// A remainder is bounded by both operands — `|a mod b| <= |a|` and
/// `|a mod b| < |b|` — so the tighter of the two integer-digit counts holds.
/// DuckDB types `DECIMAL(10,2) % DECIMAL(10,0)` as `DECIMAL(12,2)` (ten integer
/// digits) where eight suffice, because `a` itself has only eight. Recorded
/// explicitly so a future reader does not "fix" Rocky to match DuckDB.
#[test]
fn rocky_modulo_is_tighter_than_duckdb_and_sound() {
    let conn = DuckDbConnector::in_memory().expect("in-memory DuckDB");
    let dec = |precision: u8, scale: u8| RockyType::Decimal { precision, scale };

    let duck = duckdb_result_type(&conn, &operand_sql(10, 2), "%", &operand_sql(10, 0));
    let (duck_int, _) = duckdb_parts(&duck).expect("decimal");
    let rocky = arithmetic_result_type(DecimalOp::Modulo, &dec(10, 2), &dec(10, 0));
    let (rocky_int, rocky_scale) = decimal_parts_of(&rocky).expect("decimal");

    assert_eq!(rocky, dec(10, 2));
    assert!(
        rocky_int < duck_int,
        "premise: Rocky ({rocky}) is tighter than DuckDB ({duck}) here"
    );
    // Soundness: the left operand has only 8 integer digits, and a remainder
    // never exceeds the dividend in magnitude.
    let (left_int, left_scale) = decimal_parts_of(&dec(10, 2)).expect("decimal");
    assert!(
        rocky_int >= left_int && rocky_scale >= left_scale,
        "the exact remainder is bounded by |a|, which needs {left_int} int digits"
    );
}

/// Two places where DuckDB's own result type is **narrower than the exact
/// result**, documented as measured fact.
///
/// This is the evidence for the design's governing position: matching the
/// warehouse and being sound are different targets, and Rocky picks sound. If
/// these ever start agreeing with Rocky, the divergence rationale in
/// `arithmetic_result_type` needs revisiting.
#[test]
fn duckdb_under_provisions_where_rocky_is_deliberately_wider() {
    let conn = DuckDbConnector::in_memory().expect("in-memory DuckDB");
    let dec = |precision: u8, scale: u8| RockyType::Decimal { precision, scale };

    // Multiply: the exact product of two DECIMAL(10,2) needs 16 integer digits.
    let duck = duckdb_result_type(&conn, &operand_sql(10, 2), "*", &operand_sql(10, 2));
    let (duck_int, _) = duckdb_parts(&duck).expect("decimal");
    let rocky = arithmetic_result_type(DecimalOp::Multiply, &dec(10, 2), &dec(10, 2));
    let (rocky_int, _) = decimal_parts_of(&rocky).expect("decimal");
    assert_eq!(rocky, dec(20, 4));
    assert!(
        rocky_int > duck_int,
        "Rocky ({rocky}) should be wider than DuckDB ({duck}) on multiply"
    );

    // Mixed-scale add: DuckDB's DECIMAL(10,0) + DECIMAL(10,10) result type
    // cannot hold its own inputs, and DuckDB raises a range error at run time
    // for values its own type claims to accept.
    let duck = duckdb_result_type(&conn, &operand_sql(10, 0), "+", &operand_sql(10, 10));
    let (duck_int, _) = duckdb_parts(&duck).expect("decimal");
    assert!(
        duck_int < 10,
        "premise: DuckDB under-provisions integer digits here (got {duck})"
    );
    assert_eq!(
        arithmetic_result_type(DecimalOp::Add, &dec(10, 0), &dec(10, 10)),
        dec(21, 10),
        "Rocky preserves all 10 integer digits plus the carry"
    );
}
