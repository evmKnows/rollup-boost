mod dict;
mod stream;
mod zstd;

pub use stream::*;
pub use zstd::dict_id;

pub mod test_internals {
    pub use crate::dict::{load_from_disk, now, sha256_hex};
}
