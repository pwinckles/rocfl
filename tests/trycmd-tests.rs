#[test]
fn fs_tests() {
    trycmd::TestCases::new().case("tests/trycmd/fs/*.toml");
}
