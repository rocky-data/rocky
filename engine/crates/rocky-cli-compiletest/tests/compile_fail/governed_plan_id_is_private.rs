//! An external consumer must not be able to name `plan_store::governed_plan_id` —
//! it could persist or pre-name a plan without crossing the governed
//! propose gate.

fn main() {
    let _ = rocky_cli::plan_store::governed_plan_id;
}
