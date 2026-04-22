"""
merutable_viz — visualization helpers for the merutable Jupyter notebook.

Pure Python. No Rust compilation dependency.
"""

import json
import os
import struct
from pathlib import Path

import pyarrow.parquet as pq


def _display_graphviz(dot):
    """Display a Graphviz object, falling back to DOT source if `dot` binary is missing."""
    try:
        from IPython.display import display, SVG
        svg_str = dot.pipe(format="svg", encoding="utf-8")
        display(SVG(svg_str))
    except Exception:
        # Fallback: print DOT source when Graphviz binaries aren't installed.
        print(dot.source)
    return dot


# ── Graphviz LSM tree rendering ──────────────────────────────────────────────

def render_tree(stats: dict, title: str = "merutable LSM state"):
    """
    Render LSM tree as a Graphviz DOT graph: memtable → L0 → L1 → ...
    Each file node shows size, row count, and DV indicator.

    Returns an IPython-displayable Graphviz object.
    """
    try:
        import graphviz
    except ImportError:
        print("pip install graphviz")
        return None

    dot = graphviz.Digraph(comment=title)
    dot.attr(rankdir="TB", fontname="Helvetica", bgcolor="white")
    dot.attr("node", fontname="Helvetica", fontsize="10")
    dot.attr("edge", fontname="Helvetica", fontsize="9")

    # Memtable node.
    mem = stats["memtable"]
    mem_label = (
        f"Memtable\n"
        f"{_fmt_bytes(mem['active_size_bytes'])} / {_fmt_bytes(mem['flush_threshold'])}\n"
        f"{mem['active_entry_count']} entries"
    )
    if mem["immutable_count"] > 0:
        mem_label += f"\n{mem['immutable_count']} immutable"
    dot.node("memtable", mem_label,
             shape="box", style="filled", fillcolor="#E3F2FD",
             color="#1565C0", penwidth="2")

    prev_cluster = "memtable"
    for level_info in stats["levels"]:
        level = level_info["level"]
        cluster_name = f"cluster_L{level}"
        level_id = f"L{level}"

        with dot.subgraph(name=cluster_name) as sg:
            sg.attr(label=f"Level {level}  ({_fmt_bytes(level_info['total_bytes'])})",
                    style="dashed", color="#78909C", fontcolor="#37474F")

            for i, f in enumerate(level_info["files"]):
                node_id = f"L{level}_f{i}"
                fname = Path(f["path"]).name[:12]
                dv_tag = " 🗑️" if f["has_dv"] else ""
                label = f"{fname}{dv_tag}\n{f['num_rows']} rows · {_fmt_bytes(f['file_size'])}"
                fill = "#FFF3E0" if f["has_dv"] else "#E8F5E9"
                border = "#E65100" if f["has_dv"] else "#2E7D32"
                sg.node(node_id, label, shape="box", style="filled,rounded",
                        fillcolor=fill, color=border)

        # Edge from previous level.
        first_file = f"L{level}_f0"
        dot.edge(prev_cluster, first_file, style="dashed", color="#90A4AE")
        prev_cluster = first_file

    return _display_graphviz(dot)


def render_htap_flow():
    """
    Static Graphviz DOT: merutable write → Parquet on disk → DuckDB SQL read.
    """
    try:
        import graphviz
    except ImportError:
        print("pip install graphviz")
        return None

    dot = graphviz.Digraph("External-reads flow")
    dot.attr(rankdir="LR", fontname="Helvetica", bgcolor="white")
    dot.attr("node", fontname="Helvetica", fontsize="11")

    dot.node("app", "Application\nput() / get()", shape="box",
             style="filled", fillcolor="#E3F2FD", color="#1565C0")
    dot.node("meru", "merutable\nRust LSM Engine", shape="box",
             style="filled", fillcolor="#E8F5E9", color="#2E7D32")
    dot.node("pq", "Parquet Files\n(L0 / L1 / L2+)", shape="cylinder",
             style="filled", fillcolor="#FFF3E0", color="#E65100")
    dot.node("iceberg", "Iceberg v3\nManifest + DVs", shape="note",
             style="filled", fillcolor="#F3E5F5", color="#6A1B9A")
    dot.node("duck", "DuckDB / Spark / Trino\nSELECT * FROM ...", shape="box",
             style="filled", fillcolor="#FCE4EC", color="#C62828")

    dot.edge("app", "meru", label="put(row)")
    dot.edge("meru", "pq", label="flush / compact")
    dot.edge("meru", "iceberg", label="snapshot commit")
    dot.edge("pq", "duck", label="read_parquet()", color="#C62828")
    dot.edge("iceberg", "duck", label="metadata", style="dashed", color="#6A1B9A")

    return _display_graphviz(dot)


# ── Matplotlib ────────────────────────────────────────────────────────────────

def encoding_table():
    """
    Render the level-aware Parquet tuning matrix as a matplotlib table.
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("pip install matplotlib")
        return

    fig, ax = plt.subplots(figsize=(10, 2.5))
    ax.axis("off")
    ax.set_title("Level-Aware Parquet Tuning", fontsize=13, fontweight="bold", pad=12)

    data = [
        ["L0",  "4 MiB",   "8 KiB",   "PLAIN (all)",                    "Rowstore — point lookups"],
        ["L1",  "32 MiB",  "32 KiB",  "Per-column (see below)",         "Warm — transitional"],
        ["L2+", "128 MiB", "128 KiB", "Per-column (analytics)",         "Columnstore — scans"],
    ]
    cols = ["Level", "Row Group", "Page Size", "Encoding", "Tuning biased for"]

    table = ax.table(cellText=data, colLabels=cols, loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.0, 1.6)

    # Style header row.
    for j in range(len(cols)):
        table[0, j].set_facecolor("#37474F")
        table[0, j].set_text_props(color="white", fontweight="bold")

    # Alternate row colors.
    for i in range(1, len(data) + 1):
        color = "#E3F2FD" if i % 2 == 1 else "#FFFFFF"
        for j in range(len(cols)):
            table[i, j].set_facecolor(color)

    plt.tight_layout()
    plt.show()


def benchmark_chart(results: dict):
    """
    Dual-panel chart: write throughput (left) and read latency (right).

    results: {
        "write": {"merutable": [...]},
        "read":  {"merutable": [...]},
        "sizes": [1000, 10000, 50000, 100000],
    }
    """
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("pip install matplotlib numpy")
        return

    sizes = results["sizes"]
    engines = []
    colors = []
    for name, color in [("merutable", "#2E7D32"), ("sqlite", "#1565C0"), ("rocksdb", "#E65100")]:
        if results["write"].get(name) is not None:
            engines.append(name)
            colors.append(color)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Left: write throughput (rows/sec).
    x = np.arange(len(sizes))
    width = 0.5 if len(engines) == 1 else 0.8 / len(engines)
    for i, (name, color) in enumerate(zip(engines, colors)):
        vals = results["write"][name]
        offset = 0 if len(engines) == 1 else i * width - 0.4 + width / 2
        bars = ax1.bar(x + offset, vals, width,
                       label=name, color=color, alpha=0.85)
        # Add value labels on top of bars
        for bar, val in zip(bars, vals):
            ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                     f"{val:,}", ha="center", va="bottom", fontsize=8)

    ax1.set_xlabel("Dataset Size (rows)")
    ax1.set_ylabel("Rows/sec")
    ax1.set_title("Write Throughput (put_batch)", fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{s:,}" for s in sizes])
    ax1.grid(axis="y", alpha=0.3)

    # Right: read latency (µs/lookup).
    for i, (name, color) in enumerate(zip(engines, colors)):
        vals = results["read"][name]
        offset = 0 if len(engines) == 1 else i * width - 0.4 + width / 2
        bars = ax2.bar(x + offset, vals, width,
                       label=name, color=color, alpha=0.85)
        for bar, val in zip(bars, vals):
            ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                     f"{val}", ha="center", va="bottom", fontsize=8)

    ax2.set_xlabel("Dataset Size (rows)")
    ax2.set_ylabel("µs / lookup")
    ax2.set_title("Point Read Latency (get)", fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"{s:,}" for s in sizes])
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.show()


# ── Parquet file anatomy ─────────────────────────────────────────────────────

def file_anatomy(parquet_path: str):
    """
    Inspect a merutable Parquet file: row groups, pages, encodings,
    footer KV metadata (bloom filter, KvSparseIndex).
    """
    meta = pq.read_metadata(parquet_path)
    schema = pq.read_schema(parquet_path)

    print(f"{'─' * 60}")
    print(f"File: {Path(parquet_path).name}")
    print(f"{'─' * 60}")
    print(f"  Rows:       {meta.num_rows:,}")
    print(f"  Row groups: {meta.num_row_groups}")
    print(f"  Columns:    {meta.num_columns}")
    print(f"  Size:       {_fmt_bytes(meta.serialized_size)}")
    print()

    # Column names.
    col_names = [schema.field(i).name for i in range(len(schema))]
    print(f"  Columns: {col_names}")
    print()

    # Per row-group, per-column: encoding and page stats.
    for rg_idx in range(meta.num_row_groups):
        rg = meta.row_group(rg_idx)
        print(f"  Row Group {rg_idx}: {rg.num_rows:,} rows, {_fmt_bytes(rg.total_byte_size)}")
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            enc = col.encodings
            comp = col.compression
            print(f"    {col_names[col_idx]:30s}  enc={enc}  comp={comp}  "
                  f"size={_fmt_bytes(col.total_compressed_size)}")
    print()

    # Footer KV metadata.
    # pyarrow returns metadata keys/values as bytes.
    kv = meta.metadata
    if kv:
        print("  Footer KV metadata:")
        for raw_key in sorted(kv.keys()):
            raw_val = kv[raw_key]
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
            if key.startswith("merutable."):
                print(f"    {key}: {len(raw_val)} bytes")
            else:
                try:
                    decoded = raw_val.decode("utf-8") if isinstance(raw_val, bytes) else str(raw_val)
                    if len(decoded) > 200:
                        decoded = decoded[:200] + "..."
                    print(f"    {key}: {decoded}")
                except (UnicodeDecodeError, AttributeError):
                    print(f"    {key}: {len(raw_val)} bytes (binary)")
    else:
        print("  (no footer KV metadata)")
    print(f"{'─' * 60}")


def inspect_bloom(parquet_path: str):
    """
    Extract and describe the bloom filter from a merutable Parquet footer KV.
    """
    meta = pq.read_metadata(parquet_path)
    kv = meta.metadata or {}

    bloom_key = None
    for k in kv:
        k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
        if "bloom" in k_str.lower():
            bloom_key = k
            break

    if bloom_key is None:
        print("  No bloom filter found in footer KV.")
        return

    blob = kv[bloom_key]
    if isinstance(blob, str):
        blob = blob.encode("latin-1")
    key_str = bloom_key.decode("utf-8") if isinstance(bloom_key, bytes) else str(bloom_key)
    print(f"  Bloom filter key: {key_str}")
    print(f"  Blob size: {len(blob)} bytes")

    # merutable bloom is FastLocalBloom: cache-line-aligned, 64-byte buckets.
    # First 4 bytes: num_probes (u32 LE), next 4: num_buckets (u32 LE).
    if len(blob) >= 8:
        num_probes = struct.unpack("<I", blob[:4])[0]
        num_buckets = struct.unpack("<I", blob[4:8])[0]
        print(f"  num_probes: {num_probes}")
        print(f"  num_buckets: {num_buckets} (× 64 bytes = {num_buckets * 64:,} bytes)")
        bits = num_buckets * 64 * 8
        print(f"  total bits: {bits:,}")


def inspect_kv_index(parquet_path: str):
    """
    Extract and describe the KvSparseIndex from a merutable Parquet footer KV.
    """
    meta = pq.read_metadata(parquet_path)
    kv = meta.metadata or {}

    idx_key = None
    for k in kv:
        k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
        if "kv_index" in k_str.lower():
            idx_key = k
            break

    if idx_key is None:
        print("  No KvSparseIndex found in footer KV.")
        return

    blob = kv[idx_key]
    if isinstance(blob, str):
        blob = blob.encode("latin-1")
    key_str = idx_key.decode("utf-8") if isinstance(idx_key, bytes) else str(idx_key)
    print(f"  KvSparseIndex key: {key_str}")
    print(f"  Blob size: {len(blob)} bytes")

    # KvSparseIndex header: num_entries (u32 LE), restart_interval (u32 LE).
    if len(blob) >= 8:
        num_entries = struct.unpack("<I", blob[:4])[0]
        restart_interval = struct.unpack("<I", blob[4:8])[0]
        print(f"  num_entries: {num_entries}")
        print(f"  restart_interval: {restart_interval}")


def inspect_puffin(puffin_path: str):
    """
    Parse a Puffin file and show its structure: magic, blobs, footer JSON.
    """
    data = Path(puffin_path).read_bytes()

    print(f"{'─' * 60}")
    print(f"Puffin file: {Path(puffin_path).name}")
    print(f"{'─' * 60}")
    print(f"  Size: {_fmt_bytes(len(data))}")

    # Check PFA1 magic at start and end.
    magic = b"PFA1"
    if data[:4] == magic:
        print(f"  Header magic: PFA1 ✓")
    else:
        print(f"  Header magic: {data[:4].hex()} ✗")

    if data[-4:] == magic:
        print(f"  Footer magic: PFA1 ✓")
    else:
        print(f"  Footer magic: {data[-4:].hex()} ✗")

    # Footer length is i32 LE at offset [-8:-4].
    if len(data) >= 8:
        footer_len = struct.unpack("<i", data[-8:-4])[0]
        print(f"  Footer length: {footer_len} bytes")

        footer_start = len(data) - 8 - footer_len
        if 0 <= footer_start < len(data):
            footer_json = data[footer_start:footer_start + footer_len]
            try:
                footer = json.loads(footer_json)
                print(f"  Footer JSON:")
                for blob_info in footer.get("blobs", []):
                    print(f"    blob_type: {blob_info.get('type', '?')}")
                    print(f"    offset: {blob_info.get('offset', '?')}")
                    print(f"    length: {blob_info.get('length', '?')}")
                    props = blob_info.get("properties", {})
                    if props:
                        for k, v in props.items():
                            print(f"    {k}: {v}")
            except json.JSONDecodeError:
                print(f"  Footer: (not valid JSON)")

    # DV blob magic check.
    if len(data) > 12:
        dv_magic = bytes([0xD1, 0xD3, 0x39, 0x64])
        # Search for DV magic after the PFA1 header.
        blob_start = 4  # after PFA1
        if blob_start + 8 <= len(data):
            # DV envelope: length(4 BE) + magic(4).
            potential_magic = data[blob_start + 4:blob_start + 8]
            if potential_magic == dv_magic:
                dv_len = struct.unpack(">I", data[blob_start:blob_start + 4])[0]
                print(f"  DV magic: D1D33964 ✓")
                print(f"  DV blob length: {dv_len} bytes")

    print(f"{'─' * 60}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_bytes(n: int) -> str:
    """Human-readable byte size."""
    if n < 1024:
        return f"{n} B"
    elif n < 1024 * 1024:
        return f"{n / 1024:.1f} KiB"
    elif n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MiB"
    else:
        return f"{n / (1024 * 1024 * 1024):.1f} GiB"
