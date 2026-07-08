pub use slozhn_proto::slozhn::v1;

#[cfg(test)]
mod tests {
    use super::v1::{frame, Frame, Metadata, Open};
    use crate::ext::MetadataExt;
    use prost::Message as _;

    #[test]
    fn frame_roundtrip() {
        let frame = Frame {
            stream_id: 1,
            seq: 0,
            kind: Some(frame::Kind::Open(Open {
                method: "/test.Svc/Do".into(),
                metadata: Some(Metadata::ascii("k", "v")),
            })),
        };
        let bytes = frame.encode_to_vec();
        let decoded = Frame::decode(bytes.as_slice()).unwrap();
        assert_eq!(frame, decoded);
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        // protocol evolution: an old decoder reads a frame with a new field
        let mut bytes = Frame { stream_id: 7, seq: 0, kind: None }.encode_to_vec();
        // field 99, varint 5 — "unknown field from a future version"
        bytes.extend_from_slice(&[0xF8, 0x31, 0x05]);
        let decoded = Frame::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.stream_id, 7);
    }
}
