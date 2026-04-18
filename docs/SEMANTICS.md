# merutable semantics reference

Precise statements about what the engine does and does not guarantee.
This file exists to ground ambiguous README wording against the code
that actually runs in production. When the README and this file
disagree, this file wins and the README is wrong.

## Deletion Vectors: read-side live, write-side dormant

**Read-side (live).** On every compaction merge, each input file is
opened with its associated DV, if any, and the DV's marked row
positions are filtered out before the merge iterator sees them.
Implementation: `crates/merutable-engine/src/compaction/job.rs`
(`open_source_file`). This is load-bearing for Apache Iceberg v3
interop — an external writer (Spark, pyiceberg, Trino) is permitted
to stamp a DV on a data file that merutable owns, and merutable will
honor it on the next merge that touches the file. Removing the
read-side would break v3 compatibility.

**Write-side (dormant).** The API for constructing a DV-stamping
commit exists and is verified: `SnapshotTransaction::add_dv(path,
dv)` in `crates/merutable-iceberg/src/snapshot.rs`, backed by the
Puffin v3 `deletion-vector-v1` encoder in
`crates/merutable-iceberg/src/deletion_vector.rs`, with the
post-union cardinality validation performed in
`IcebergCatalog::commit` (IMP-17). The full path round-trips in the
catalog test suite.

**No production caller of the write-side exists.** Every compaction
is a full rewrite — input files are fully consumed and removed via
`txn.remove_file(path)`. There is no residual source file on which
a DV could be stamped. Consequently the `deletion-vector-v1` blob
type is never emitted as output of any merutable-owned write.

**Why this is intentional.** Full-rewrite compaction is the simpler,
lower-read-amplification model. Partial compaction (the mode that
would exercise the write-side) trades ~10× lower write amplification
at deep levels for ~1.5–3× higher read amplification on queries that
touch a partially-rewritten file. That trade is workload-dependent,
not universally better. The decision to keep merutable full-rewrite-
only is documented in the [RFC #19](https://github.com/merutable/merutable/issues/19)
outcome (stay full-rewrite-only; revisit if field data shows deep-
level write-amp pain).

**What this means for users.**

- External writers CAN stamp DVs on merutable's files; those DVs are
  honored on the next compaction.
- merutable's commits NEVER produce DVs. A vanilla Iceberg v3 reader
  looking at a merutable-only table will see zero DV blobs across
  the commit history.
- The code path for DV writes is not dead code; it is preserved for
  the day partial compaction is implemented (see RFC #19) or for
  future compatibility experiments that stamp DVs from an external
  tooling layer (e.g. row-level deletes via an admin CLI).

## MVCC semantics seen by external readers

Short version: an external reader sees the *union* of every live
Parquet file — which includes cross-level duplicates and tombstones.
An `ORDER BY seq DESC, LIMIT 1` (or `ROW_NUMBER() QUALIFY …`)
projection is required to recover the `MeruDB::get`/`scan`-equivalent
view. Full treatment is in [docs/HTAP_READS.md](HTAP_READS.md) once
that file lands; until then see the `IcebergCatalog::commit` and
`read_path` modules.

## Full-rewrite invariants

Every successful compaction commit satisfies:

- Every file in the picked input set is either in the new snapshot's
  `status: deleted` marker list, or is physically absent from the
  manifest. There is no "partially live" state.
- L1+ files are non-overlapping within each level (the picker pulls
  in every overlapping L(k+1) file in full; the invariant is
  structural).
- The output file set is fsynced before the manifest rename commits.
- The row cache is invalidated at commit.

See the compaction job in `crates/merutable-engine/src/compaction/job.rs`
for the authoritative sequence.
