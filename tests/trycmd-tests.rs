use std::env;

#[test]
fn fs_tests() {
    env::set_var("TZ", "UTC");
    trycmd::TestCases::new().case("tests/trycmd/fs/*.toml");
    env::remove_var("TZ");
}
