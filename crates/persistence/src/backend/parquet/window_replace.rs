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

use arrow::array::{BooleanArray, UInt64Array};
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

pub(crate) struct WindowReplace {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) compression: Option<parquet::basic::Compression>,
    pub(crate) max_row_group_size: Option<usize>,
    pub(crate) directory: ObjectPath,
    /// Marker-visible object listing of the directory (collected via the
    /// catalog's `list_objects` so pending publications are already resolved).
    pub(crate) visible: Vec<ObjectMeta>,
}

impl WindowReplace {
    /// Replaces every row in `[window.0, window.1]` (by `ts_init`, inclusive
    /// bounds) of the directory's parquet files with `replacement`.
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
    /// `ts_init` column, projection-incompatible source schemas.
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
            let Some((file_start, file_end)) = paths::parse_filename_timestamps(&name) else {
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
                let mask = outside_window_mask(&batch, window)?;
                if mask.iter().any(|keep| keep == Some(true)) {
                    kept_batches.push(filter_record_batch(&batch, &mask)?);
                }
            }
            sources.push(path.clone());
        }

        // Sources whose entire contents fall inside the window contribute no
        // kept rows and are still removed (fully replaced).
        let mut combined: Vec<RecordBatch> = kept_batches;
        combined.extend(replacement);
        let sorted = sort_by_ts_init(&combined, &target_schema)?;

        let (min_ts, max_ts) = sorted
            .iter()
            .filter_map(ts_init_bounds)
            .reduce(|acc, cur| (acc.0.min(cur.0), acc.1.max(cur.1)))
            .ok_or_else(|| anyhow::anyhow!("no rows survived replacement"))?;

        let file_name = paths::timestamps_to_filename(
            nautilus_core::UnixNanos::from(min_ts),
            nautilus_core::UnixNanos::from(max_ts),
        );
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
            None,
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

/// Keep-mask for rows with `ts_init` strictly outside `[window.0, window.1]`.
fn outside_window_mask(batch: &RecordBatch, window: (u64, u64)) -> anyhow::Result<BooleanArray> {
    let ts = batch
        .column_by_name("ts_init")
        .ok_or_else(|| anyhow::anyhow!("batch lacks ts_init column"))?;
    let values = ts
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow::anyhow!("ts_init is not UInt64"))?;
    Ok(values
        .iter()
        .map(|v| v.map(|ts| ts < window.0 || ts > window.1))
        .collect())
}

fn ts_init_bounds(batch: &RecordBatch) -> Option<(u64, u64)> {
    let ts = batch.column_by_name("ts_init")?;
    let values = ts.as_any().downcast_ref::<UInt64Array>()?;
    let mut min: Option<u64> = None;
    let mut max: Option<u64> = None;
    for v in values.iter().flatten() {
        min = Some(min.map_or(v, |m| m.min(v)));
        max = Some(max.map_or(v, |m| m.max(v)));
    }
    Some((min?, max?))
}

fn sort_by_ts_init(
    batches: &[RecordBatch],
    schema: &SchemaRef,
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
                    .column_by_name("ts_init")
                    .expect("ts_init checked")
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
}
