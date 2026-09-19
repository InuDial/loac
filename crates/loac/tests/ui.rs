#[test]
// Trybuild spawns rustc subprocesses, which Miri cannot execute.
#[cfg_attr(miri, ignore)]
fn public_type_contract() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/fail/*.rs");
}
