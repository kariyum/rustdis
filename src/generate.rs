use rand::RngExt;

use crate::{Generate, ResponseBody};

pub fn handle_generate(seed: String, request_body: Generate) -> ResponseBody {
    let mut rng = rand::rng();
    let random_number: u32 = rng.random();
    ResponseBody::GenerateOk {
        in_reply_to: request_body.msg_id,
        id: format!("{}-{}", seed, random_number),
    }
}
