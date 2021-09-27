mod util;
pub mod bitcask;

#[cfg(test)]
mod tests {
    use super::bitcask::*;

    #[test]
    fn index_update() {
        let mut index = Index::new();
        assert_eq!(
            index.update(vec![0, 1, 2, 3, 4], IndexValue::new(0, 0, 5, 0)).is_none(),
            true
        );
        assert_eq!(
            index.update(vec![0, 1, 2, 3, 4], IndexValue::new(0, 0, 10, 100)).is_some(),
            true
        );
    }

    #[test]
    fn index_lookup() {
        let mut index = Index::new();
        assert_eq!(
            index.lookup(vec![0, 1, 2, 3, 4]).is_err(),
            true,
        );
        index.update(vec![0, 1, 2, 3, 4], IndexValue::new(0, 0, 5, 0));
        assert_eq!(
            index.lookup(vec![0, 1, 2, 3, 4]).unwrap(),
            IndexValue::new(0, 0, 5, 0)
        );
    }

    #[test]
    fn entry_checksums() {
        let key = "Hello".as_bytes().to_vec();
        let value = "Yoted".as_bytes().to_vec();
        let mut entry = Entry::new(key.clone(), value);
        let mut other_entry = Entry::new(key, "Toted".as_bytes().to_vec());

        println!("{:?}", entry.as_bytes());

        // Compare to another object 
        let checksum = entry.calculate_checksum();
        assert_ne!(checksum, other_entry.calculate_checksum());   

        // Change the entry and compare the checksums
        entry.key = "I CHANGED!".as_bytes().to_vec();
        assert_ne!(checksum, entry.calculate_checksum());
    }

    #[test]
    fn writer_can_write() {
        let mut writer = Writer::new("/tmp/yoted".to_string()).expect("Should open a writer");

        let key = "Hello".as_bytes().to_vec();
        let value = "Yoted".as_bytes().to_vec();
        let entry = Entry::new(key.clone(), value);
        writer.insert(entry).expect("Can insert an entry");
    }
}
