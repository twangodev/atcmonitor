import hashlib
from pathlib import Path
from typing import Union

cache_dir = Path("./cache")
cache_dir.mkdir(parents=True, exist_ok=True)

def _hash_key(key: str) -> str:
    """Return a filesystem-safe filename for any string key."""
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return h

def read_cache(key: str) -> bytes | None:
    """
    Read cached data by key.
    Returns the raw string, or None if not found.
    """
    fname = cache_dir / _hash_key(key)
    try:
        return fname.read_bytes()
    except FileNotFoundError:
        return None

def write_cache(key: str, data: Union[str, bytes], extension: str = "") -> None:
    """
    Write data to cache under its hashed name.
    Accepts either bytes (any binary) or str (will be UTF-8 encoded).
    """
    file_hash = _hash_key(key)
    if extension:
        fname = cache_dir / f"{file_hash}.{extension}"
    else:
        fname = cache_dir / file_hash
    fname.parent.mkdir(parents=True, exist_ok=True)

    fname.write_bytes(data)