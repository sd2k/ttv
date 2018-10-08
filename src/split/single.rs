use std::ops::Deref;
use std::str::FromStr;

use crate::error::{Error, Result};

/// Represents a single 'split' of data
pub trait Split {
    /// Get the name of the split.
    fn name(&self) -> &str;
}

/// A split based on a proportion.
#[derive(Clone, Debug)]
pub struct ProportionSplit {
    /// The split name. Will be used as the filename for the split.
    name: String,
    /// The proportion of data that should be directed to this split.
    pub proportion: f64,
}

impl Split for ProportionSplit {
    fn name(&self) -> &str {
        return &self.name;
    }
}

impl FromStr for ProportionSplit {
    type Err = Error;

    /// Create a ProportionSplit from a string specification, such as
    /// "train=0.8".
    fn from_str(spec: &str) -> Result<Self> {
        let split: Vec<&str> = spec.split('=').collect();
        if split.len() != 2 {
            return Err(Error::InvalidSplitSpecification(spec.to_string()));
        }
        let proportion = split[1]
            .parse::<f64>()
            .map_err(|_| Error::InvalidSplitSpecification(spec.to_string()))?;
        if proportion <= 0.0 || proportion >= 1.0 {
            return Err(Error::InvalidSplitSpecification(spec.to_string()));
        }
        Ok(ProportionSplit {
            name: split[0].to_string(),
            proportion,
        })
    }
}

/// A split based on a number of rows
#[derive(Clone, Debug)]
pub struct RowSplit {
    /// The split name. Will be used as the filename for the split.
    name: String,
    /// The total number of rows to send to this split.
    /// Stored as an f64 for optimization reasons.
    pub total: f64,
    /// The number of rows sent to this split so far.
    pub done: f64,
}

impl Split for RowSplit {
    fn name(&self) -> &str {
        return &self.name;
    }
}

impl FromStr for RowSplit {
    type Err = Error;

    /// Create a ProportionSplit from a string specification, such as
    /// "train=0.8".
    fn from_str(spec: &str) -> Result<Self> {
        let split: Vec<&str> = spec.split('=').collect();
        if split.len() != 2 {
            return Err(Error::InvalidSplitSpecification(spec.to_string()));
        }
        let total = split[1]
            .parse::<u64>()
            .map(|total| total as f64)
            .map_err(|_| Error::InvalidSplitSpecification(spec.to_string()))?;
        Ok(RowSplit {
            name: split[0].to_string(),
            total,
            done: 0.0,
        })
    }
}

pub enum SplitEnum {
    Rows(RowSplit),
    Proportion(ProportionSplit),
}

impl Deref for SplitEnum {
    type Target = Split;
    fn deref(&self) -> &Self::Target {
        match self {
            SplitEnum::Rows(r) => r,
            SplitEnum::Proportion(p) => p,
        }
    }
}
