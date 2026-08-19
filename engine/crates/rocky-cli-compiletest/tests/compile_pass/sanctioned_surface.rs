//! Positive control: the sanctioned surface IS nameable — the same
//! module the failing cases import from, so a privacy failure there is
//! attributable to the privatization and nothing else.

fn main() {
    // Reading a plan grants no authority; it stays public.
    let _read = rocky_cli::plan_store::read_plan;
    // The loop's one sanctioned route to a persisted AI-authored plan.
    let _propose = rocky_cli::commands::propose_governed_run_plan;
}
