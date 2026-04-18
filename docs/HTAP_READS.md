# HTAP read contract for external engines

This document defines the correct read-time projection for external
SQL engines (DuckDB, Spark, Trino, Snowflake, Athena) consuming a
merutable table via its exported Iceberg v2 `metadata.json`. **Every
external reader MUST apply this projection.** A naive `SELECT *`
returns MVCC duplicates and tombstones as if they were valid distinct
rows — silent wrong answers.

This contract is structurally unavoidable for any LSM-based HTAP
table. It is not a temporary gap that future work removes. See
[RFC #19](https://github.com/merutable/merutable/issues/19) for why
Deletion Vectors cannot replace it.

## Go through the Iceberg manifest, never glob raw Parquet

```python
# WRONG — picks up files still in GC grace window, bypasses status=deleted
duckdb.sql(f"SELECT * FROM read_parquet('{db.catalog_path()}/data/L*/*.parquet')")

# RIGHT — Iceberg-aware readers respect the manifest
db.export_iceberg("/tmp/events-iceberg")
duckdb.sql(f"SELECT * FROM iceberg_scan('/tmp/events-iceberg/metadata/v1.metadata.json')")
```

Raw `read_parquet` globs return files that:
- Compaction has already removed from the manifest but are still on
  disk during the `gc_grace_period_secs` window (default 300 s).
- Belong to a snapshot other than the one the user queried.

Iceberg-aware readers filter these out via the manifest's `status:
deleted` markers and snapshot boundaries. Raw globs do not.

## Apply the MVCC dedup projection

```sql
SELECT * EXCLUDE (_merutable_ikey)
FROM iceberg_table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY <primary_key_columns>
    ORDER BY merutable_seq(_merutable_ikey) DESC
) = 1
   AND merutable_op(_merutable_ikey) = 'Put';
```

Why this is mandatory:

1. **Cross-L0 duplicates** — two memtable flushes can both contain the
   same PK at different sequence numbers; both end up as distinct
   rows in the raw file union.
2. **Cross-level duplicates** — L0 may hold the newest version of a
   key while L1 still carries an older version from a prior compaction.
   Both are visible until the next L0→L1 merge collapses them.
3. **Tombstones** — `db.delete(pk)` writes a row with `op_type = Delete`
   and non-PK columns set to NULL or sentinels. Without the `_op =
   'Put'` filter, the tombstone surfaces as a valid row with all-NULL
   non-PK columns — indistinguishable from a user who wrote a real row
   with all-NULL non-PK columns.

Engines that recognize the sort-order metadata emitted by
`db.export_iceberg` (see [#20](https://github.com/merutable/merutable/issues/20))
apply this as a streaming "first row per partition" filter at O(N)
cost; engines that don't pay an O(N log N) sort. Either way the
projection is required — the cost difference is only in the plan.

## The `merutable_seq` / `merutable_op` UDFs

`_merutable_ikey` is a `BINARY` column whose last 8 bytes encode
`(seq << 8) | op_type`. `merutable_seq` / `merutable_op` unpack that
trailer:

```python
import duckdb
duckdb.sql("""
CREATE OR REPLACE MACRO merutable_seq(ikey) AS
    (get_byte(ikey, length(ikey) - 8)::BIGINT << 48)
  | (get_byte(ikey, length(ikey) - 7)::BIGINT << 40)
  | (get_byte(ikey, length(ikey) - 6)::BIGINT << 32)
  | (get_byte(ikey, length(ikey) - 5)::BIGINT << 24)
  | (get_byte(ikey, length(ikey) - 4)::BIGINT << 16)
  | (get_byte(ikey, length(ikey) - 3)::BIGINT << 8)
  |  get_byte(ikey, length(ikey) - 2)::BIGINT;

CREATE OR REPLACE MACRO merutable_op(ikey) AS
    CASE get_byte(ikey, length(ikey) - 1)
        WHEN 1 THEN 'Put'
        WHEN 2 THEN 'Delete'
    END;
""")
```

(Spark / Trino equivalents are mechanical translations of the same
byte arithmetic. Issue #16 proposes emitting `_seq` and `_op` as
typed columns directly in the Parquet schema so no UDF is needed —
tracking follow-up.)

## `primary_key_columns`

These are exactly the columns declared `primary_key: [i, j, ...]` in
the `TableSchema` passed to `MeruDB::open`. They're the natural
`PARTITION BY`. The Iceberg metadata exported by merutable includes
an `identifier-field-ids` entry that enumerates them; Iceberg-aware
engines can surface this to users.

## Snapshot isolation

Each `db.flush()` or `db.compact()` commit produces a new Iceberg
snapshot. `db.export_iceberg(target_dir)` writes a `v{N}.metadata.json`
and a `version-hint.text` pointing at it. External readers that want
stable, repeatable results should pin to a specific `metadata.json`
path rather than re-reading the `version-hint.text` on every query.

## Summary

- Go through the manifest.
- Apply the dedup projection.
- Engines that understand merutable's sort order get it for free (O(N));
  engines that don't pay the sort (O(N log N)).
- Typed `_seq` / `_op` columns are tracked in [#16](https://github.com/merutable/merutable/issues/16).
