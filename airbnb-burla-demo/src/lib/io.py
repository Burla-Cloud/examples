"""Stage I/O helpers: parquet read/write with input-hash checkpointing."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def register_src_for_burla() -> None:
    """Force-register all src.* modules with cloudpickle so Burla workers can
    unpickle them.

    Burla auto-classifies modules as "burla internal" if their __spec__.origin
    contains the substring "burla". The repo path
    /Users/.../agents/airbnb-burla/src/... matches that test by accident, so
    Burla never calls cloudpickle.register_pickle_by_value on our src modules
    and the worker side fails to import them. Registering them here pickles by
    value, sidestepping that bug.

    Call this from every stage's main() before remote_parallel_map.
    """
    import cloudpickle
    for name in list(sys.modules.keys()):
        if name == "src" or name.startswith("src."):
            mod = sys.modules.get(name)
            if mod is not None:
                try:
                    cloudpickle.register_pickle_by_value(mod)
                except (TypeError, ValueError):
                    pass


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def input_hash(*items: Any) -> str:
    """Stable hash of inputs (paths, configs, integer counts) for resume-checks."""
    h = hashlib.sha256()
    for it in items:
        if isinstance(it, Path):
            try:
                h.update(it.read_bytes() if it.is_file() else str(it.resolve()).encode())
            except Exception:
                h.update(str(it).encode())
        elif isinstance(it, (dict, list, tuple)):
            h.update(json.dumps(it, sort_keys=True, default=str).encode())
        else:
            h.update(str(it).encode())
    return h.hexdigest()[:16]


def write_parquet(df: pd.DataFrame, path: Path, *, input_hash_value: Optional[str] = None,
                  metadata: Optional[Mapping[str, str]] = None) -> None:
    """Write a DataFrame to parquet with input_hash baked into file metadata.

    pandas .attrs do not survive a parquet round-trip, so we store the hash
    in pyarrow schema-level metadata where stage_done() can find it.
    """
    ensure_dir(path.parent)
    table = pa.Table.from_pandas(df, preserve_index=False)
    md = dict(table.schema.metadata or {})
    if input_hash_value:
        md[b"input_hash"] = input_hash_value.encode()
    if metadata:
        for k, v in metadata.items():
            md[k.encode() if isinstance(k, str) else k] = (v.encode() if isinstance(v, str) else v)
    table = table.replace_schema_metadata(md)
    pq.write_table(table, path, compression="zstd")


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def read_parquet_metadata(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    md = pq.read_schema(path).metadata or {}
    return {k.decode() if isinstance(k, bytes) else k:
            v.decode() if isinstance(v, bytes) else v for k, v in md.items()}


def stage_done(path: Path, input_hash_value: Optional[str] = None) -> bool:
    """True if the stage's output parquet is present and matches the input hash.

    If input_hash_value is None, just checks for existence.
    """
    if not path.exists():
        return False
    if input_hash_value is None:
        return True
    md = read_parquet_metadata(path)
    return md.get("input_hash") == input_hash_value


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, default=str))


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())
