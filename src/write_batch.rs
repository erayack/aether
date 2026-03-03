use std::{collections::HashMap, mem::size_of};

use crate::types::{BatchOp, Key, Value, WriteBatch};

impl WriteBatch {
    pub fn put(&mut self, key: Key, value: Value) {
        self.ops.push(BatchOp::Put { key, value });
    }

    pub fn delete(&mut self, key: Key) {
        self.ops.push(BatchOp::Delete { key });
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    #[must_use]
    pub fn approx_bytes(&self) -> usize {
        self.ops.iter().fold(0_usize, |acc, op| {
            let op_bytes = match op {
                BatchOp::Put { key, value } => key
                    .len()
                    .saturating_add(value.len())
                    .saturating_add(size_of::<u8>())
                    .saturating_add(size_of::<u32>() * 2),
                BatchOp::Delete { key } => key
                    .len()
                    .saturating_add(size_of::<u8>())
                    .saturating_add(size_of::<u32>()),
            };
            acc.saturating_add(op_bytes)
        })
    }

    #[must_use]
    pub fn canonicalize_last_write_wins(self) -> Self {
        if self.ops.len() <= 1 {
            return self;
        }

        let mut last_index_by_key = HashMap::with_capacity(self.ops.len());
        for (index, op) in self.ops.iter().enumerate() {
            let key = match op {
                BatchOp::Put { key, .. } | BatchOp::Delete { key } => key.clone(),
            };
            last_index_by_key.insert(key, index);
        }

        let mut canonical_ops = Vec::with_capacity(last_index_by_key.len());
        for (index, op) in self.ops.into_iter().enumerate() {
            let key = match &op {
                BatchOp::Put { key, .. } | BatchOp::Delete { key } => key,
            };
            if last_index_by_key.get(key).copied() == Some(index) {
                canonical_ops.push(op);
            }
        }

        Self { ops: canonical_ops }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::types::{BatchOp, WriteBatch};

    #[test]
    fn builder_put_delete_and_empty_state() {
        let mut batch = WriteBatch::default();
        assert!(batch.is_empty(), "default batch should be empty");

        batch.put(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        batch.delete(Bytes::from_static(b"k2"));

        assert!(!batch.is_empty(), "batch with ops should not be empty");
        assert_eq!(
            batch.ops,
            vec![
                BatchOp::Put {
                    key: Bytes::from_static(b"k1"),
                    value: Bytes::from_static(b"v1"),
                },
                BatchOp::Delete {
                    key: Bytes::from_static(b"k2"),
                },
            ]
        );
    }

    #[test]
    fn approx_bytes_matches_op_encoding_estimate() {
        let mut batch = WriteBatch::default();
        batch.put(Bytes::from_static(b"ab"), Bytes::from_static(b"xyz"));
        batch.delete(Bytes::from_static(b"k"));

        // Put: key(2) + value(3) + kind(1) + key_len(4) + value_len(4) = 14
        // Delete: key(1) + kind(1) + key_len(4) = 6
        assert_eq!(batch.approx_bytes(), 20);
    }

    #[test]
    fn canonicalize_keeps_only_last_op_per_key() {
        let batch = WriteBatch {
            ops: vec![
                BatchOp::Put {
                    key: Bytes::from_static(b"a"),
                    value: Bytes::from_static(b"1"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"a"),
                    value: Bytes::from_static(b"2"),
                },
                BatchOp::Delete {
                    key: Bytes::from_static(b"a"),
                },
            ],
        };

        let canonical = batch.canonicalize_last_write_wins();
        assert_eq!(
            canonical.ops,
            vec![BatchOp::Delete {
                key: Bytes::from_static(b"a"),
            }]
        );
    }

    #[test]
    fn canonicalize_preserves_order_of_last_occurrences() {
        let batch = WriteBatch {
            ops: vec![
                BatchOp::Put {
                    key: Bytes::from_static(b"a"),
                    value: Bytes::from_static(b"1"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"b"),
                    value: Bytes::from_static(b"1"),
                },
                BatchOp::Delete {
                    key: Bytes::from_static(b"a"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"c"),
                    value: Bytes::from_static(b"1"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"b"),
                    value: Bytes::from_static(b"2"),
                },
            ],
        };

        let canonical = batch.canonicalize_last_write_wins();
        assert_eq!(
            canonical.ops,
            vec![
                BatchOp::Delete {
                    key: Bytes::from_static(b"a"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"c"),
                    value: Bytes::from_static(b"1"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"b"),
                    value: Bytes::from_static(b"2"),
                },
            ]
        );
    }
}
