# merutable

[![CI](https://github.com/merutable/merutable/actions/workflows/ci.yml/badge.svg)](https://github.com/merutable/merutable/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-stable-blue.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

An embeddable Rust HTAP database engine. One logical table backed by an LSM-tree where manifests are Apache Iceberg v3 snapshots and SSTables are Apache Parquet files. Row promotions between LSM levels use Iceberg v3 Deletion Vectors (Puffin format) instead of physical deletes. Every commit (flush or compaction) produces a spec-compliant Iceberg v3 snapshot readable by Spark, Trino, and DuckDB — no ETL, no format conversion.

Named after the [Meru Parvatha](https://en.wikipedia.org/wiki/Mount_Meru) from Indian mythology.

## Why merutable

- **HTAP in one binary**: Transactional writes (put/delete/scan) with sub-millisecond memtable lookups, while the on-disk format is standard Iceberg+Parquet readable by any analytics engine.
- **No ETL pipeline**: Every commit (flush or compaction) produces a spec-compliant Iceberg v3 snapshot. Analytical readers see consistent data at any snapshot without a separate ingestion step.
- **Deletion Vectors, not physical deletes**: Promoted rows are masked via Puffin-format roaring bitmaps (Iceberg v3 `deletion-vector-v1`). Source files remain readable throughout compaction.
- **SIMD-optimized bloom filter**: AVX2/NEON runtime-dispatched cache-line-aligned bloom filter for fast negative lookups on the read path.
- **Prefix-compressed sparse index**: Each Parquet file carries a `KvSparseIndex` in the footer KV — a front-coded `user_key → page_location` map with binary-searchable restart points (LevelDB/RocksDB index-block style). Point lookups binary-search the restarts then linear-scan at most one restart interval, skipping all non-matching pages. Full keys, no 64-byte truncation.
- **Pluggable storage**: Local filesystem for development, S3 with LRU disk cache for production.

## Architecture

```
         put/delete/get/scan
                |
         [ MeruDB API ]
                |
         [ MeruEngine ]
           /    |    \
     [WAL]  [Memtable]  [VersionSet]
              |               |
         [FlushJob]    [IcebergTable]
              |               |
       [ParquetWriter]  [Manifest + DV]
              |               |
       [Bloom + KvSparseIndex] [Puffin files]
              |
         [ObjectStore]
              |
       L0/ L1/ L2/ ... (Parquet files)
```

`IcebergTable` manages a single Iceberg v3 table (manifests, snapshots, version-hint). Catalog integration (Hive, Glue, REST, etc.) is an external layer on top — merutable provides the table, not the catalog.

**Write path**: Sequence assign → WAL append → memtable insert → flush when threshold crossed. Each flush produces a new Iceberg v3 snapshot.

**Read path**: Memtable (active + immutable queue) → L0 files (bloom → `KvSparseIndex` page skip → scan) → L1..LN (bloom → `KvSparseIndex` → binary search).

**Compaction**: Leveled compaction with Deletion Vector tracking. Fully compacted files are removed from the manifest; partially compacted files get a DV update. Each compaction commit is a new Iceberg snapshot — external readers (Spark, Trino, DuckDB) always see a consistent, spec-compliant table at any snapshot.

## Crate map

| Crate | Responsibility |
|---|---|
| `merutable-types` | `InternalKey` encoding, `TableSchema`, `FieldValue`, `SeqNum`, `OpType`, `MeruError` |
| `merutable-wal` | 32 KiB block format WAL with CRC32, recovery, rotation |
| `merutable-memtable` | `crossbeam` skip-list memtable, `bumpalo` arena, rotation, flow control |
| `merutable-parquet` | Parquet SSTable writer/reader, `FastLocalBloom`, `KvSparseIndex`, footer KV metadata |
| `merutable-iceberg` | Iceberg v3 table management: manifest, snapshots, `VersionSet` (ArcSwap), `DeletionVector` (Puffin). Not a catalog — catalog integration (Hive, Glue, REST) is external. |
| `merutable-store` | Pluggable object store: local FS, S3, LRU disk cache |
| `merutable-engine` | `FlushJob`, `CompactionJob`, `MergingIterator`, read/write paths |
| `merutable` | Public embedding API: `MeruDB`, `OpenOptions`, `ScanIterator` |

## Storage tuning

The LSM tree uses level-aware Parquet tuning to serve both OLTP and OLAP workloads:

| Level | Row group | Page size | Encoding | Tuning biased for |
|-------|-----------|-----------|----------|-------------------|
| L0 | 4 MiB | 8 KiB | PLAIN (all columns) | Rowstore — point lookups, memtable flush |
| L1 | 32 MiB | 32 KiB | Per-column (see below) | Warm — transitional |
| L2+ | 128 MiB | 128 KiB | Per-column (see below) | Columnstore — analytics scans |

**Per-column encoding at L1+:**
- `_merutable_ikey` (lookup key): PLAIN — zero-overhead decode for point lookups
- `Int32`/`Int64`: DELTA_BINARY_PACKED — optimal for sorted integer columns
- `Float`/`Double`: BYTE_STREAM_SPLIT — IEEE 754 byte-transposition
- `ByteArray` (strings): RLE_DICTIONARY — high compression for categorical data
- `Boolean`: RLE

L0 files carry both `_merutable_ikey` + `_merutable_value` (postcard blob for KV fast-path) and typed columns. L1+ files drop the blob and store only `_merutable_ikey` + typed columns — the analytical format external engines read.

## Quick start

```rust
use merutable::{MeruDB, OpenOptions};
use merutable::merutable_types::schema::{TableSchema, ColumnDef, ColumnType};
use merutable::merutable_types::value::FieldValue;

#[tokio::main]
async fn main() {
    let schema = TableSchema {
        table_name: "events".into(),
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Int64, nullable: false },
            ColumnDef { name: "payload".into(), col_type: ColumnType::ByteArray, nullable: true },
        ],
        primary_key: vec![0],
    };

    let db = MeruDB::open(OpenOptions::new(schema)).await.unwrap();
    db.put(&[FieldValue::Int64(1)], &[FieldValue::Int64(1), FieldValue::Null]).await.unwrap();
    let row = db.get(&[FieldValue::Int64(1)]).unwrap();
    println!("{row:?}");
}
```

## License

Apache-2.0
