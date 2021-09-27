use core::fmt;
use std::{collections::HashMap, convert::TryInto, error::Error, fs::{File, OpenOptions}, io::{Read, Seek, Write}, sync::{Arc, Mutex}};

use crc::{Crc, CRC_64_ECMA_182};

use crate::util;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct IndexValue {
    timestamp: u128,
    file_id: usize,
    offset: usize,
    size: usize,
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
    pub fn new(timestamp: u128, file_id: usize, offset: usize, size: usize) -> Self {
        Self {
            timestamp,
            file_id,
            offset,
            size,
        }
    }
}

pub struct Index {
    keys: HashMap<Vec<u8>, IndexValue>,
}

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

// Constants
pub const CRC: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

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
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let active = true;
        let timestamp = util::get_timestamp_since_epoch();
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

    pub fn calculate_checksum(&mut self) -> u64 {
        let mut digest = CRC.digest();

        // Keep only calculate the data

        digest.update(if self.active { &[1] } else { &[0] });
        digest.update(&self.timestamp.to_ne_bytes());
        digest.update(&self.key_size.to_ne_bytes());
        digest.update(&self.value_size.to_ne_bytes());
        digest.update(&self.key);
        digest.update(&self.value);

        let checksum = digest.finalize();

        checksum
    }

    pub fn as_bytes(&mut self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        let mut active = if self.active { vec![1] } else { vec![0] };

        data.append(&mut self.checksum.to_ne_bytes().to_vec());
        data.append(&mut active);
        data.append(&mut self.timestamp.to_ne_bytes().to_vec());
        data.append(&mut self.key_size.to_ne_bytes().to_vec());
        data.append(&mut self.value_size.to_ne_bytes().to_vec());
        data.append(&mut self.key.clone());
        data.append(&mut self.value.clone());

        data
    }

    pub fn from_reader(file: &mut File) -> Result<Self, Box<dyn std::error::Error>>  {
        let mut buf: [u8; 64] = [0; 64];

        file.read(&mut buf[0..8])?;
        let checksum = u64::from_ne_bytes(buf[0..8].try_into().unwrap());
        
        file.read(&mut buf[0..1])?;
        let active = if  buf[0] == 1 { true } else { false }; 

        file.read(&mut buf[0..16])?;
        let timestamp = u128::from_ne_bytes(buf[0..16].try_into().unwrap());

        file.read(&mut buf[0..8])?;
        let key_size = usize::from_ne_bytes(buf[0..8].try_into().unwrap());

        file.read(&mut buf[0..8])?;
        let value_size = usize::from_ne_bytes(buf[0..8].try_into().unwrap());

        let mut key = Vec::new();
        let mut value = Vec::new();
        key.resize(key_size, 0);
        value.resize(value_size, 0);

        file.read(&mut key[0..key_size])?;
        file.read(&mut value[0..value_size])?;

        Ok(
            Entry {
                checksum,
                active,
                timestamp,
                key_size,
                value_size,
                key,
                value
            }
        )
    }

    pub fn mark_inactive(&mut self) {
        self.active = false;
    }
}

#[allow(dead_code)]
pub struct Writer {
    index: Arc<Mutex<Index>>,
    file: Arc<Mutex<File>>,
    directory: String,
}

impl Writer {
    pub fn new(directory: String) -> Result<Self, Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&directory)?;
    
        Ok(
            Self {
                index: Arc::new(Mutex::new(Index::new())),
                file: Arc::new(Mutex::new(file)),
                directory,
            }
        )
    }

    pub fn insert(&mut self, entry: Entry) -> Result<(), Box<dyn std::error::Error>> {
        let index = self.index.lock().unwrap();
        let mut file = self.file.lock().unwrap();

        // let data = entry.as_bytes();
        // index.update("Hello".as_bytes().to_vec(), IndexValue::new(0, 0, 0, data.len()));
        // file.write_all(&entry.clone().as_bytes()).unwrap();

        match index.lookup(entry.key.clone()) {
            Ok(value) => {
                let _ = file.seek(std::io::SeekFrom::Start(value.offset as u64)).unwrap();
                
                let mut found_entry = Entry::from_reader(&mut file)?;
                found_entry.mark_inactive();

                println!("Found entry: {:?}", found_entry);

            },
            Err(_) => {
                println!("New entry: {:?}", entry);

                file.write_all(&entry.clone().as_bytes()).unwrap();
            }
        };

        Ok(())
    }
}
