#![deny(clippy::all)]
#![allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::return_self_not_must_use,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]

pub mod error;
pub mod key;
pub mod level;
pub mod schema;
pub mod sequence;
pub mod value;

pub use error::{MeruError, Result};
