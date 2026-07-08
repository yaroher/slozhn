use crate::proto::v1::{metadata_entry, Metadata, MetadataEntry, Status};

pub trait StatusExt: Sized {
    fn ok() -> Self;
    fn with_code(code: u32, message: &str) -> Self;
}

impl StatusExt for Status {
    fn ok() -> Self {
        Self { code: 0, message: String::new(), trailers: Some(Metadata::empty()) }
    }

    fn with_code(code: u32, message: &str) -> Self {
        Self { code, message: message.to_owned(), trailers: Some(Metadata::empty()) }
    }
}

pub trait MetadataExt: Sized {
    fn empty() -> Self;
    fn ascii(key: &str, value: &str) -> Self;
}

impl MetadataExt for Metadata {
    fn empty() -> Self {
        Self { entries: Vec::new() }
    }

    fn ascii(key: &str, value: &str) -> Self {
        Self {
            entries: vec![MetadataEntry {
                key: key.to_owned(),
                value: Some(metadata_entry::Value::Ascii(value.to_owned())),
            }],
        }
    }
}
