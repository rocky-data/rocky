//! An external consumer must not be able to name `plan_store::write_plan` —
//! it could persist or pre-name a plan without crossing the governed
//! propose gate.

fn main() {
    let _ = rocky_cli::plan_store::write_plan;
}
