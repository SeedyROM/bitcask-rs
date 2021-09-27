//! Simple global utilities

use std::time::{SystemTime, UNIX_EPOCH};

/// Get the amount of micro seconds since Unix Epoch
pub fn get_micros_since_epoch() -> u128 { 
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_micros()
}