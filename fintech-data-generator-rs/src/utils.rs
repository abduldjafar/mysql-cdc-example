use rand::RngExt;

pub fn generate_nik() -> String {
    let mut rng = rand::rng();
    let mut nik = String::with_capacity(16);
    for _ in 0..16 {
        nik.push_str(&rng.random_range(0..=9).to_string());
    }
    nik
}
