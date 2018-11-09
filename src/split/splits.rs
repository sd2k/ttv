use std::ops::Deref;

use rand::{prelude::*, prng::ChaChaRng};
use try_from::TryFrom;

use crate::error::{Error, Result};
use crate::split::single::{ProportionSplit, RowSplit, Split};

pub enum SplitSelection<'a> {
    Some(&'a str),
    None,
    Done,
}

pub trait SplitSelector {
    fn get_split(&mut self, rng: &mut ChaChaRng) -> SplitSelection;
}

/// Splits defined using proportions.
#[derive(Debug, Default)]
pub struct ProportionSplits {
    pub splits: Vec<ProportionSplit>,
}

impl SplitSelector for ProportionSplits {
    fn get_split(&mut self, rng: &mut ChaChaRng) -> SplitSelection {
        let random: f64 = rng.gen();
        let mut total = 0.0;
        for split in &self.splits {
            total += split.proportion;
            if random < total {
                return SplitSelection::Some(&split.name());
            }
        }
        return SplitSelection::None;
    }
}

impl Deref for ProportionSplits {
    type Target = Vec<ProportionSplit>;
    fn deref(&self) -> &Self::Target {
        &self.splits
    }
}

impl TryFrom<Vec<ProportionSplit>> for ProportionSplits {
    type Err = Error;
    fn try_from(splits: Vec<ProportionSplit>) -> Result<Self> {
        let total = splits.iter().fold(0.0, |x, p| x + p.proportion);
        if total > 1.0 {
            return Err(Error::InvalidSplits(splits));
        }
        Ok(ProportionSplits { splits })
    }
}

/// Splits defined using rows.
#[derive(Debug, Default)]
pub struct RowSplits {
    pub splits: Vec<RowSplit>,
    /// The total number of rows in all splits combined
    total: f64,
}

impl SplitSelector for RowSplits {
    fn get_split(&mut self, rng: &mut ChaChaRng) -> SplitSelection {
        let random: f64 = rng.gen();
        let random = random * self.total;

        let mut total = 0.0;
        let unfinished_splits: Vec<&mut RowSplit> = self
            .splits
            .iter_mut()
            .filter(|s| s.done < s.total)
            .collect();

        for split in unfinished_splits.into_iter() {
            total += split.total;
            if random < total {
                split.done += 1.0;
                if split.done >= split.total {
                    self.total -= split.total;
                }
                return SplitSelection::Some(split.name().as_ref());
            }
        }
        return SplitSelection::Done;
    }
}

impl Deref for RowSplits {
    type Target = Vec<RowSplit>;
    fn deref(&self) -> &Self::Target {
        &self.splits
    }
}

impl From<Vec<RowSplit>> for RowSplits {
    fn from(splits: Vec<RowSplit>) -> Self {
        let total = splits.iter().fold(0.0, |x, y| x + y.total);
        RowSplits { splits, total }
    }
}

/// Either RowSplits or ProportionSplits, determined at runtime depending
/// on the user's input.
pub enum Splits {
    Rows(RowSplits),
    Proportions(ProportionSplits),
}

impl Deref for Splits {
    type Target = SplitSelector;
    fn deref(&self) -> &Self::Target {
        match self {
            Splits::Rows(r) => r,
            Splits::Proportions(r) => r,
        }
    }
}

impl Splits {
    /// Get a random split.
    pub fn get_split(&mut self, rng: &mut ChaChaRng) -> SplitSelection {
        match self {
            Splits::Rows(rows) => rows.get_split(rng),
            Splits::Proportions(rows) => rows.get_split(rng),
        }
    }
}
