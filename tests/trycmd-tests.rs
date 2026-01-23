use std::env;

#[test]
fn fs_tests() {
    unsafe { env::set_var("TZ", "UTC"); }
    trycmd::TestCases::new().case("tests/trycmd/fs/*.toml");
    unsafe { env::remove_var("TZ"); }
}
