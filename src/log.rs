//! 🍻 A bit cask implementation for Rust.
//!
//! ## Example:
//! ```no_run
//! use bitcask::log::*;
//!
//! let mut writer = Writer::new("/tmp/db".to_string()).expect("Should open a writer");
//!
//! let key = "Hello".as_bytes().to_vec();
//! let value = "Yoted".as_bytes().to_vec();
//! let entry = Entry::new(key, value);
//! writer.insert(entry).expect("Can insert an entry");
//! ````

use core::fmt;
use std::{collections::HashMap, convert::TryInto, error::Error, fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}, sync::{Arc, Mutex}};

use crc::{Crc, CRC_64_ECMA_182};

use crate::util::{self, get_micros_since_epoch};

/// A seek only pointer into our logs
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct IndexValue {
    timestamp: u128,
    file_id: usize,
    offset: u64,
    size: u64,
}

impl Ord for IndexValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}
impl PartialOrd for IndexValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl IndexValue {
    pub fn new(timestamp: u128, file_id: usize, offset: u64, size: u64) -> Self {
        Self {
            timestamp,
            file_id,
            offset,
            size,
        }
    }
}

/// Our in memory map of pointers to our log
#[derive(Debug)]
pub struct Index {
    keys: HashMap<Vec<u8>, IndexValue>,
}

/// The key in our index was not found
#[derive(Debug)]
pub struct IndexKeyNotFoundError(Vec<u8>);
impl Error for IndexKeyNotFoundError {}
impl fmt::Display for IndexKeyNotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key \"{:?}\" not found in index", self.0)
    }
}

impl Index {
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Update or insert an index
    pub fn update(&mut self, key: Vec<u8>, value: IndexValue) -> Option<IndexValue> {
        self.keys.insert(key, value)
    }

    /// Lookup an index, fail if the key doesn't exist
    pub fn lookup(&self, key: Vec<u8>) -> Result<IndexValue, IndexKeyNotFoundError> {
        match self.keys.get_key_value(&key) {
            Some((_key, value)) => Ok(*value),
            None => Err(IndexKeyNotFoundError(key as Vec<u8>)),
        }
    }
}

/// CRC64 digester
pub const CRC: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

/// An entry in our log which can be read and written to our log
#[derive(Debug, Clone)]
pub struct Entry {
    checksum: u64,
    active: bool,

    timestamp: u128,
    key_size: usize,
    value_size: usize,

    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Entry {
    /// Create a new entry
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let active = true;
        let timestamp = util::get_micros_since_epoch();
        let key_size = key.len();
        let value_size = value.len();
        let key = key;
        let value = value;

        let mut new_entry = Self {
            checksum: 0,
            active,
            timestamp,
            key_size,
            value_size,
            key,
            value,
        };

        // Calculate the checksum of our entry
        new_entry.checksum = new_entry.calculate_checksum();

        new_entry
    }

    // Get the checksum of or struct by digesting all the bytes besides the CRC itself
    pub fn calculate_checksum(&mut self) -> u64 {
        let mut digest = CRC.digest();

        digest.update(if self.active { &[1] } else { &[0] });
        digest.update(&self.timestamp.to_le_bytes());
        digest.update(&self.key_size.to_le_bytes());
        digest.update(&self.value_size.to_le_bytes());
        digest.update(&self.key);
        digest.update(&self.value);

        digest.finalize()
    }

    /// Converts the Entry struct into a Vec<u8> in little endian form.
    pub fn as_bytes(&mut self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        let mut active = if self.active { vec![1] } else { vec![0] };

        data.append(&mut self.checksum.to_le_bytes().to_vec());
        data.append(&mut active);
        data.append(&mut self.timestamp.to_le_bytes().to_vec());
        data.append(&mut self.key_size.to_le_bytes().to_vec());
        data.append(&mut self.value_size.to_le_bytes().to_vec());
        data.append(&mut self.key.clone());
        data.append(&mut self.value.clone());

        data
    }

    /// Takes in a file and from the specific offset retrieves and builds an Entry struct
    pub fn from_reader(file: &mut File) -> Result<Self, Box<dyn std::error::Error>> {
        let mut buf: [u8; 16] = [0; 16]; // Max non dynamic field size is 16 bytes, use this

        file.read(&mut buf[0..8])?;
        let checksum = u64::from_le_bytes(buf[0..8].try_into().unwrap());

        file.read(&mut buf[0..1])?;
        let active = if buf[0] == 1 { true } else { false };

        file.read(&mut buf[0..16])?;
        let timestamp = u128::from_le_bytes(buf[0..16].try_into().unwrap());

        file.read(&mut buf[0..8])?;
        let key_size = usize::from_le_bytes(buf[0..8].try_into().unwrap());

        file.read(&mut buf[0..8])?;
        let value_size = usize::from_le_bytes(buf[0..8].try_into().unwrap());

        let mut key = Vec::new();
        let mut value = Vec::new();
        key.resize(key_size, 0);
        value.resize(value_size, 0);

        file.read(&mut key[0..key_size])?;
        file.read(&mut value[0..value_size])?;

        Ok(Entry {
            checksum,
            active,
            timestamp,
            key_size,
            value_size,
            key,
            value,
        })
    }

    /// Mark the entry as inactive so we can compact it later
    pub fn mark_inactive(&mut self) {
        self.active = false;
    }
}

/// Writes append only data to our log file and manages stale data
pub struct Writer {
    index: Arc<Mutex<Index>>,
    file: Arc<Mutex<File>>,
    pub offset: Arc<Mutex<u64>>,
    #[allow(dead_code)]
    directory: String,
}

impl Writer {
    /// Create a new writer with a given "directory" (it's a file for now...)
    pub fn new(directory: String) -> Result<Self, Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&directory)?;

        // TODO: Reconstruct the data from disk
        // TODO: WTF to do here lol

        Ok(Self {
            index: Arc::new(Mutex::new(Index::new())),
            file: Arc::new(Mutex::new(file)),
            directory,
            offset: Arc::new(Mutex::new(0)),
        })
    }

    /// Insert an entry into our writer IO
    pub fn insert(&mut self, mut entry: Entry) -> Result<(), Box<dyn std::error::Error>> {
        let mut index = self.index.lock().unwrap();
        let mut file = self.file.lock().unwrap();

        match index.lookup(entry.key.clone()) {
            Ok(value) => {
                // Seek to the found entry from the index
                let _ = file
                    .seek(SeekFrom::Start(value.offset as u64))
                    .unwrap();

                // Read the entry struct from the index, and then mark it inactive
                let mut found_entry = Entry::from_reader(&mut file)?;
                found_entry.mark_inactive();
                let found_data_size: i64 = found_entry.as_bytes().to_vec().len() as i64;
                let _ = file.seek(SeekFrom::Current(-found_data_size))?;
                file.write_all(&found_entry.as_bytes().to_vec()).unwrap();

                log::trace!("Found and deactivated entry: {:?}", found_entry);

                // Seek to the end
                let _ = file
                    .seek(SeekFrom::End(0))
                    .unwrap();
                
                // Get the offset
                let mut offset = self.offset.lock().unwrap();
                // Get the new offset from our current position in the file
                let current_offset = file.stream_position()?;

                // Update the entry in our index
                let data = entry.as_bytes();
                index.update(
                    entry.key.clone(),
                    IndexValue::new(get_micros_since_epoch(), 0, current_offset, data.len() as u64),
                );
                // Write the data and update our writers offset
                file.write_all(&data).unwrap();
                *offset = current_offset;

                log::trace!("Create updated entry: {:?}", entry);

                // TODO: Append to the log
            }
            Err(_) => {
                log::trace!("New entry: {:?}", entry);

                // Jump to the offset
                let mut current_offset = self.offset.lock().unwrap();
                let _ = file
                    .seek(SeekFrom::Start(*current_offset))
                    .unwrap();
                
                // append our data
                let data = entry.as_bytes();
                index.update(
                    entry.key.clone(),
                    IndexValue::new(get_micros_since_epoch(), 0, *current_offset, data.len() as u64),
                );
                file.write_all(&data).unwrap();
                // Update the offset
                *current_offset += data.len() as u64;
            }
        };

        Ok(())
    }

    /// Get an entry from the writer
    // TODO: This should probably be called something else?
    pub fn get(&mut self, key: Vec<u8>) -> Result<Entry, Box<dyn std::error::Error>> {
        let index = self.index.lock().unwrap();
        let mut file = self.file.lock().unwrap();
        let index_entry = index.lookup(key)?;

        let _ = file
            .seek(std::io::SeekFrom::Start(index_entry.offset as u64))
            .unwrap();

        let found_entry = Entry::from_reader(&mut file)?;

        Ok(found_entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_update() {
        let mut index = Index::new();
        assert_eq!(
            index
                .update(vec![0, 1, 2, 3, 4], IndexValue::new(0, 0, 5, 0))
                .is_none(),
            true
        );
        assert_eq!(
            index
                .update(vec![0, 1, 2, 3, 4], IndexValue::new(0, 0, 10, 100))
                .is_some(),
            true
        );
    }

    #[test]
    fn index_lookup() {
        let mut index = Index::new();
        assert_eq!(index.lookup(vec![0, 1, 2, 3, 4]).is_err(), true,);
        index.update(vec![0, 1, 2, 3, 4], IndexValue::new(0, 0, 5, 0));
        assert_eq!(
            index.lookup(vec![0, 1, 2, 3, 4]).unwrap(),
            IndexValue::new(0, 0, 5, 0)
        );
    }

    #[test]
    fn entry_checksums() {
        let key = "Hello".as_bytes().to_vec();
        let value = "Bloated".as_bytes().to_vec();
        let mut entry = Entry::new(key.clone(), value);
        let mut other_entry = Entry::new(key, "Toted".as_bytes().to_vec());

        // Compare to another object
        let checksum = entry.calculate_checksum();
        assert_ne!(checksum, other_entry.calculate_checksum());

        // Change the entry and compare the checksums
        entry.key = "I CHANGED!".as_bytes().to_vec();
        assert_ne!(checksum, entry.calculate_checksum());
    }

    #[test]
    fn writer_crud() {
        let mut writer = Writer::new("/tmp/db".to_string()).expect("Should open a writer");

        let key = "Hello".as_bytes().to_vec();
        let key2 = "Yump".as_bytes().to_vec();
        let key3 = "Joyous".as_bytes().to_vec();
        let value = "Jinkies".as_bytes().to_vec();
        let value2 = "I am new, and I am not old".as_bytes().to_vec();
        let value3 = "I am older, and I am not deeper than new".as_bytes().to_vec();

        let mut entry = Entry::new(key.clone(), value.clone());
        writer.insert(entry).expect("Can insert an entry");

        // Insert a new index
        entry = Entry::new(key2.clone(), value2.clone());
        writer.insert(entry.clone()).expect("Can insert another entry");

        // // This should be ignored because it's the same value, maybe this is a bad idea?
        entry = Entry::new(key3.clone(), value3.clone());
        writer.insert(entry.clone()).expect("Can insert another * 3 entry");

        // Get the newest version of the entry
        let mut found_entry = writer.get(key.clone()).expect("Found the updated key from our log file");
        assert_eq!(found_entry.value.clone(), value.clone());

        found_entry = writer.get(key2.clone()).expect("Found the updated key from our log file");
        assert_eq!(found_entry.value.clone(), value2.clone());

        found_entry = writer.get(key3.clone()).expect("Found the updated key from our log file");
        assert_eq!(found_entry.value.clone(), value3.clone());
    }
}
