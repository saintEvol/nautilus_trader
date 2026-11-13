//! Window replacement over a catalog directory — the narrow publication
//! primitive for patching overlapping history segments
//! (`POLYMARKET_PREDICT` §7.1: Octopus owns window selection, completeness,
//! deduplication and source choice; this module owns file-set replacement
//! visibility and crash recovery).
//!
//! Reuses the existing consolidation marker protocol (`begin`/`recover`) —
//! no second publication ledger. The caller supplies already-normalized
//! replacement batches for a closed window and the marker-visible object
//! listing; rows of intersecting files OUTSIDE the window are preserved and
//! re-published together with the replacement, so the patched directory
//! never loses out-of-window history and never shows both old and new
//! segments.

use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, UInt64Array};
use arrow::compute::{
    concat_batches, filter_record_batch, lexsort_to_indices, SortColumn, SortOptions,
    take_record_batch,
};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::backend::parquet::{
    consolidation_state, io::write_batches_to_object_store_with_mode, paths,
};

/// Segment filename + timestamp-column conventions of a flat parquet
/// directory. The codec fixes the unit of the `[start, end]` window: window
/// bounds, timestamp column values and filename intervals all compare in the
/// codec's unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentCodec {
    /// Nautilus catalog convention: `{iso}_{iso}[_{identity}].parquet`
    /// filenames encoding Unix **nanoseconds**; timestamp column `ts_init`
    /// (`UInt64`).
    NautilusNanos,
    /// Plain epoch-millis convention: `{start_ms}-{end_ms}.parquet`
    /// filenames; timestamp column `ts_ms` (`Int64`).
    EpochMillis,
}

impl SegmentCodec {
    /// Timestamp column the window compares against. Both signed and
    /// unsigned 64-bit integer columns are accepted; values must be
    /// non-negative (post-1970 event times).
    #[must_use]
    pub fn ts_column(self) -> &'static str {
        match self {
            Self::NautilusNanos => "ts_init",
            Self::EpochMillis => "ts_ms",
        }
    }

    /// Filename interval for a segment, in the codec's unit. Foreign names
    /// return `None` (never touched by a patch).
    #[must_use]
    pub fn parse_filename(self, name: &str) -> Option<(u64, u64)> {
        match self {
            Self::NautilusNanos => paths::parse_filename_timestamps(name),
            Self::EpochMillis => {
                let span = name.strip_suffix(".parquet")?;
                let (a, b) = span.split_once('-')?;
                Some((a.parse().ok()?, b.parse().ok()?))
            }
        }
    }

    /// Segment filename spanning `[start, end]`, in the codec's unit.
    #[must_use]
    pub fn filename(self, start: u64, end: u64) -> String {
        match self {
            Self::NautilusNanos => paths::timestamps_to_filename(
                nautilus_core::UnixNanos::from(start),
                nautilus_core::UnixNanos::from(end),
            ),
            Self::EpochMillis => format!("{start}-{end}.parquet"),
        }
    }
}

pub(crate) struct WindowReplace {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) compression: Option<parquet::basic::Compression>,
    pub(crate) max_row_group_size: Option<usize>,
    pub(crate) directory: ObjectPath,
    /// Marker-visible object listing of the directory (collected via the
    /// catalog's `list_objects` so pending publications are already resolved).
    pub(crate) visible: Vec<ObjectMeta>,
    pub(crate) codec: SegmentCodec,
    /// File-level key/value metadata stamped onto the patched segment
    /// (caller-owned layouts may require their readers' metadata contract).
    pub(crate) key_value_metadata: Option<Vec<parquet::file::metadata::KeyValue>>,
}

impl WindowReplace {
    /// Replaces every row in `[window.0, window.1]` (by the codec's
    /// timestamp column, inclusive bounds) of the directory's parquet files
    /// with `replacement`.
    ///
    /// Contract:
    /// - files whose filename interval is disjoint from the window are left
    ///   untouched;
    /// - intersecting files become publication sources: their out-of-window
    ///   rows are retained and written together with the replacement into a
    ///   single new file spanning the combined minimum/maximum timestamps;
    /// - publication is atomic per the marker protocol: until the new file
    ///   lands, readers see the old file set; after it lands (and pending
    ///   cleanup), they see only the replacement result — never both.
    ///
    /// # Errors
    ///
    /// Loudly rejects: empty replacement (a window may honestly have data —
    /// use deletion semantics for removal, not a no-op patch), missing
    /// timestamp column, projection-incompatible source schemas.
    pub(crate) async fn replace_window(
        &self,
        window: (u64, u64),
        replacement: Vec<RecordBatch>,
    ) -> anyhow::Result<()> {
        let directory = self.directory.clone();
        // Consume any pending publication from an interrupted earlier run so
        // the caller's visible listing and our writes start settled.
        consolidation_state::recover(&self.store, &directory).await?;

        let replacement_rows: usize = replacement.iter().map(RecordBatch::num_rows).sum();
        anyhow::ensure!(
            replacement_rows > 0,
            "window replacement requires at least one replacement row \
             (empty windows are deletions, not patches)"
        );

        // Union schema: the replacement defines the target; sources must be
        // projection-compatible (same names, same types).
        let target_schema: SchemaRef = replacement
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| anyhow::anyhow!("replacement empty"))?;

        let mut sources: Vec<ObjectPath> = Vec::new();
        let mut kept_batches: Vec<RecordBatch> = Vec::new();

        for meta in &self.visible {
            let path = &meta.location;
            if path.as_ref().ends_with('/') {
                continue;
            }
            let name = path
                .parts()
                .next_back()
                .map(|s| s.as_ref().to_string())
                .unwrap_or_default();
            if !name.ends_with(".parquet") {
                continue;
            }
            let Some((file_start, file_end)) = self.codec.parse_filename(&name) else {
                // Non-timestamped foreign file: never touch it.
                continue;
            };
            if file_end < window.0 || file_start > window.1 {
                continue; // disjoint: untouched
            }

            let (batches, schema) = read_parquet(&self.store, path).await?;
            anyhow::ensure!(
                schemas_projection_equal(&schema, target_schema.as_ref()),
                "source {name} schema is not projection-compatible with the \
                 replacement schema ({schema:?} vs {target_schema:?})"
            );
            for batch in batches {
                let mask = outside_window_mask(&batch, window, self.codec.ts_column())?;
                if mask.iter().any(|keep| keep == Some(true)) {
                    kept_batches.push(filter_record_batch(&batch, &mask)?);
                }
            }
            sources.push(path.clone());
        }

        // Sources whose entire contents fall inside the window contribute no
        // kept rows and are still removed (fully replaced).
        let ts_column = self.codec.ts_column();
        let mut combined: Vec<RecordBatch> = kept_batches;
        combined.extend(replacement);
        let sorted = sort_by_ts(&combined, &target_schema, ts_column)?;

        let (min_ts, max_ts) = sorted
            .iter()
            .filter_map(|b| ts_column_bounds(b, ts_column))
            .reduce(|acc, cur| (acc.0.min(cur.0), acc.1.max(cur.0.max(cur.1))))
            .ok_or_else(|| anyhow::anyhow!("no rows survived replacement"))?;

        let file_name = self.codec.filename(min_ts, max_ts);
        let new_path = directory.clone().join(file_name.as_str());

        // Publication: marker first, data second, source deletion last —
        // each interruption point converges via `recover`.
        let source_metas: Vec<ObjectMeta> = self
            .visible
            .iter()
            .filter(|m| sources.contains(&m.location))
            .cloned()
            .collect();
        let created_by =
            consolidation_state::begin(&self.store, &new_path, &source_metas).await?;
        write_batches_to_object_store_with_mode(
            &sorted,
            self.store.clone(),
            &new_path,
            self.compression,
            self.max_row_group_size,
            self.key_value_metadata.clone(),
            object_store::PutMode::Overwrite,
            Some(created_by),
        )
        .await?;
        for source in &sources {
            self.store.delete(source).await?;
        }
        consolidation_state::recover(&self.store, &directory).await
    }
}

async fn read_parquet(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
) -> anyhow::Result<(Vec<RecordBatch>, SchemaRef)> {
    let data = store.get(path).await?.bytes().await?;
    if data.is_empty() {
        return Ok((Vec::new(), Arc::new(Schema::empty())));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok((batches, schema))
}

/// Keep-mask for rows with the codec timestamp strictly outside
/// `[window.0, window.1]`. Unsigned and signed 64-bit columns are both
/// accepted; signed values must be non-negative (post-1970 event times).
fn outside_window_mask(
    batch: &RecordBatch,
    window: (u64, u64),
    ts_column: &str,
) -> anyhow::Result<BooleanArray> {
    let ts = batch
        .column_by_name(ts_column)
        .ok_or_else(|| anyhow::anyhow!("batch lacks {ts_column} column"))?;
    if let Some(values) = ts.as_any().downcast_ref::<UInt64Array>() {
        return Ok(values
            .iter()
            .map(|v| v.map(|ts| ts < window.0 || ts > window.1))
            .collect());
    }
    let values = ts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("{ts_column} is not an integer column"))?;
    Ok(values
        .iter()
        .map(|v| v.map(|ts| (ts as u64) < window.0 || (ts as u64) > window.1))
        .collect())
}

fn ts_column_bounds(batch: &RecordBatch, ts_column: &str) -> Option<(u64, u64)> {
    let ts = batch.column_by_name(ts_column)?;
    let mut min: Option<u64> = None;
    let mut max: Option<u64> = None;
    if let Some(values) = ts.as_any().downcast_ref::<UInt64Array>() {
        for v in values.iter().flatten() {
            min = Some(min.map_or(v, |m| m.min(v)));
            max = Some(max.map_or(v, |m| m.max(v)));
        }
    } else {
        let values = ts.as_any().downcast_ref::<Int64Array>()?;
        for v in values.iter().flatten() {
            let v = v.max(0) as u64;
            min = Some(min.map_or(v, |m| m.min(v)));
            max = Some(max.map_or(v, |m| m.max(v)));
        }
    }
    Some((min?, max?))
}

fn sort_by_ts(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    ts_column: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let non_empty: Vec<RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).cloned().collect();
    if non_empty.is_empty() {
        return Ok(Vec::new());
    }
    let combined = concat_batches(schema, &non_empty)?;
    if combined.num_rows() == 0 {
        return Ok(Vec::new());
    }
    // Stable by original row order: pre-sort indices keep same-timestamp rows
    // in their input order (kept-outside rows then replacement rows).
    let original_index = Arc::new(UInt64Array::from_iter_values(0..combined.num_rows() as u64));
    let options = Some(SortOptions {
        descending: false,
        nulls_first: false,
    });
    let indices = lexsort_to_indices(
        &[
            SortColumn {
                values: combined
                    .column_by_name(ts_column)
                    .ok_or_else(|| anyhow::anyhow!("batch lacks {ts_column} column"))?
                    .clone(),
                options,
            },
            SortColumn {
                values: original_index,
                options,
            },
        ],
        None,
    )?;
    Ok(vec![take_record_batch(&combined, &indices)?])
}

fn schemas_projection_equal(source: &Schema, target: &Schema) -> bool {
    source.fields().len() == target.fields().len()
        && source
            .fields()
            .iter()
            .zip(target.fields().iter())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
}

#[cfg(test)]
mod tests {
    //! Offline window-replacement proofs on an in-memory object store:
    //! disjoint files untouched, intersecting out-of-window rows preserved,
    //! replacement lands as one file, empty replacement refused.

    use super::*;
    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use object_store::memory::InMemory;

    fn schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("ts_init", DataType::UInt64, false),
            Field::new("value", DataType::UInt32, false),
        ]))
    }

    fn batch(schema: &SchemaRef, ts: &[u64], values: &[u32]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(ts.iter().copied())),
                Arc::new(UInt32Array::from_iter_values(values.iter().copied())),
            ],
        )
        .unwrap()
    }

    async fn write_file(
        store: &Arc<dyn ObjectStore>,
        directory: &ObjectPath,
        schema: &SchemaRef,
        ts: &[u64],
        values: &[u32],
    ) {
        let name = paths::timestamps_to_filename(
            nautilus_core::UnixNanos::from(*ts.first().unwrap()),
            nautilus_core::UnixNanos::from(*ts.last().unwrap()),
        );
        let path = directory.clone().join(name.as_str());
        let b = batch(schema, ts, values);
        write_batches_to_object_store_with_mode(
            &[b],
            store.clone(),
            &path,
            None,
            None,
            None,
            object_store::PutMode::Create,
            None,
        )
        .await
        .unwrap();
    }

    async fn visible(store: &Arc<dyn ObjectStore>, directory: &ObjectPath) -> Vec<ObjectMeta> {
        use futures::StreamExt as _;
        let prefix = ObjectPath::from(format!("{directory}/"));
        let mut stream = store.list(Some(&prefix));
        let mut objects = Vec::new();
        while let Some(object) = stream.next().await {
            objects.push(object.unwrap());
        }
        consolidation_state::visible_objects(store, objects)
            .await
            .unwrap()
    }

    fn file_names(objects: &[ObjectMeta]) -> Vec<String> {
        objects
            .iter()
            .map(|m| {
                m.location
                    .parts()
                    .next_back()
                    .map(|s| s.as_ref().to_string())
                    .unwrap_or_default()
            })
            .collect()
    }

    #[tokio::test]
    async fn replaces_window_and_preserves_outside_rows() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = ObjectPath::from("data/trade/BTC-USDT");
        let schema = schema();
        // [0,100] disjoint; [200,500] intersecting (200-299 kept, 300-500 replaced).
        write_file(&store, &dir, &schema, &[0, 100], &[1, 2]).await;
        write_file(&store, &dir, &schema, &[200, 299, 300, 500], &[3, 4, 5, 6]).await;

        let listing = visible(&store, &dir).await;
        let originals = file_names(&listing);
        let replacement = vec![batch(&schema, &[300, 400], &[90, 91])];
        WindowReplace {
            store: store.clone(),
            compression: None,
            max_row_group_size: None,
            directory: dir.clone(),
            visible: listing,
            codec: SegmentCodec::NautilusNanos,
            key_value_metadata: None,
        }
        .replace_window((300, 500), replacement)
        .await
        .unwrap();

        let after = visible(&store, &dir).await;
        let names = file_names(&after);
        // Disjoint [0,100] untouched; intersecting file replaced by one
        // spanning the combined window [200,400] (the new file is the entry
        // that was not among the originals).
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(
            names.iter().any(|n| originals.contains(n)),
            "disjoint original must remain untouched: {names:?} vs {originals:?}"
        );
        let replacement_name = names
            .iter()
            .find(|n| !originals.contains(n))
            .expect("replacement file")
            .clone();

        // Row content: kept 200/299 (values 3,4) + replacement 300/400 (90,91).
        let (batches, read_schema) = read_parquet(&store, &dir.join(replacement_name.as_str()))
            .await
            .unwrap();
        let combined = concat_batches(&read_schema, &batches).unwrap();
        let ts: Vec<u64> = combined
            .column_by_name("ts_init")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        let values: Vec<u32> = combined
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(ts, vec![200, 299, 300, 400]);
        assert_eq!(values, vec![3, 4, 90, 91]);
    }

    #[tokio::test]
    async fn empty_replacement_refused_and_original_untouched() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = ObjectPath::from("data/trade/ETH-USDT");
        let schema = schema();
        write_file(&store, &dir, &schema, &[0, 100], &[1, 2]).await;
        let listing = visible(&store, &dir).await;
        let result = WindowReplace {
            store: store.clone(),
            compression: None,
            max_row_group_size: None,
            directory: dir.clone(),
            visible: listing,
            codec: SegmentCodec::NautilusNanos,
            key_value_metadata: None,
        }
        .replace_window((0, 100), Vec::new())
        .await;
        assert!(
            result.unwrap_err().to_string().contains("deletions"),
            "empty replacement must be refused as a deletion-shaped request"
        );
        // Original untouched.
        assert_eq!(visible(&store, &dir).await.len(), 1);
    }

    /// Epoch-millis flat-directory conventions (caller-owned layouts such as
    /// the Octopus TickLake): `{start_ms}-{end_ms}.parquet` names, `ts_ms`
    /// Int64 column, window in millis.
    #[tokio::test]
    async fn replaces_window_under_epoch_millis_codec() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = ObjectPath::from("lake/POLYMARKET/trade/0xabc/SPOT");
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts_ms", DataType::Int64, false),
            Field::new("value", DataType::UInt32, false),
        ]));
        let ms_batch = |ts: &[i64], values: &[u32]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(ts.iter().copied())),
                    Arc::new(UInt32Array::from_iter_values(values.iter().copied())),
                ],
            )
            .unwrap()
        };
        let write_ms_file = |store: &Arc<dyn ObjectStore>, ts: &'static [i64], values: &'static [u32]| {
            let path = dir
                .clone()
                .join(format!("{}-{}.parquet", ts[0], ts[ts.len() - 1]));
            let b = ms_batch(ts, values);
            let store = store.clone();
            async move {
                write_batches_to_object_store_with_mode(
                    &[b],
                    store,
                    &path,
                    None,
                    None,
                    None,
                    object_store::PutMode::Create,
                    None,
                )
                .await
                .unwrap();
            }
        };
        // [1000,1100] disjoint; [1200,1500] intersecting (1200-1299 kept).
        write_ms_file(&store, &[1000, 1100], &[1, 2]).await;
        write_ms_file(&store, &[1200, 1299, 1300, 1500], &[3, 4, 5, 6]).await;

        let listing = visible(&store, &dir).await;
        let originals = file_names(&listing);
        let replacement = vec![ms_batch(&[1300, 1400], &[90, 91])];
        WindowReplace {
            store: store.clone(),
            compression: None,
            max_row_group_size: None,
            directory: dir.clone(),
            visible: listing,
            codec: SegmentCodec::EpochMillis,
            key_value_metadata: Some(vec![parquet::file::metadata::KeyValue::new(
                "exchange".to_string(),
                Some("POLYMARKET".to_string()),
            )]),
        }
        .replace_window((1300, 1500), replacement)
        .await
        .unwrap();

        let after = visible(&store, &dir).await;
        let names = file_names(&after);
        assert_eq!(names.len(), 2, "{names:?}");
        assert!(
            names.iter().any(|n| originals.contains(n)),
            "disjoint original must remain untouched: {names:?} vs {originals:?}"
        );
        let replacement_name = names
            .iter()
            .find(|n| !originals.contains(n))
            .expect("replacement file")
            .clone();
        // New file spans kept + replacement rows: [1200,1400] in millis.
        assert_eq!(replacement_name, "1200-1400.parquet");

        // Caller key/value metadata is stamped onto the patched segment.
        let kv_path = dir.join(replacement_name.as_str());
        let data = store.get(&kv_path).await.unwrap().bytes().await.unwrap();
        let kv_footer =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data)
                .unwrap()
                .metadata()
                .clone();
        let kv_exchange = kv_footer
            .file_metadata()
            .key_value_metadata()
            .and_then(|m| {
                m.iter()
                    .find(|kv| kv.key == "exchange")
                    .and_then(|kv| kv.value.clone())
            })
            .expect("patched segment carries caller kv metadata");
        assert_eq!(kv_exchange, "POLYMARKET");

        let (batches, read_schema) = read_parquet(&store, &kv_path).await.unwrap();
        let combined = concat_batches(&read_schema, &batches).unwrap();
        let ts: Vec<i64> = combined
            .column_by_name("ts_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        let values: Vec<u32> = combined
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(ts, vec![1200, 1299, 1300, 1400]);
        assert_eq!(values, vec![3, 4, 90, 91]);
    }

    #[test]
    fn segment_codec_filename_roundtrip() {
        for codec in [SegmentCodec::NautilusNanos, SegmentCodec::EpochMillis] {
            let name = codec.filename(1_000, 2_000);
            assert_eq!(
                codec.parse_filename(&name),
                Some((1_000, 2_000)),
                "{codec:?} must round-trip {name}"
            );
            assert_eq!(codec.parse_filename("foreign.parquet"), None);
        }
    }
}
