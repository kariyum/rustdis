use rand::RngExt;

use crate::ResponsePayload;

pub fn handle_generate(seed: String) -> ResponsePayload {
    let mut rng = rand::rng();
    let random_number: u32 = rng.random();
    ResponsePayload::GenerateOk {
        id: format!("{}-{}", seed, random_number),
    }
}
