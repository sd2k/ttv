#![feature(int_to_from_bytes)]

pub mod cli;
mod error;
mod io;
mod split;

pub use {
    crate::error::{Error, Result},
    crate::io::Compression,
    crate::split::SplitterBuilder,
};
