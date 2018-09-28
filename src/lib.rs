#![feature(int_to_from_bytes)]

mod error;
mod io;
mod split;

pub use {
    crate::error::{Error, Result},
    crate::split::SplitterBuilder,
};
