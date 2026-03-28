use rand::RngExt;
use std::fmt::Write as _;

pub fn generate_nik() -> String {
    let mut rng = rand::rng();
    let mut nik = String::with_capacity(16);
    for _ in 0..16 {
        // write! directly into the String — zero heap allocation per digit
        write!(nik, "{}", rng.random_range(0u8..=9)).unwrap();
    }
    nik
}
