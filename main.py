import re
from pathlib import Path
from typing import List

from vad import chunk
from whisper_enhanced import file_to_speaker

_CHUNK_SEG_RE = re.compile(r'chunk_(\d+)_seg_(\d+)\.mp3$', re.IGNORECASE)

def get_chunk_segments(chunk_path: str) -> List[str]:
    """
    Return a list of all .mp3 files named chunk_<x>_seg_<y>.mp3,
    sorted by x (chunk) then y (segment).
    """
    p = Path(chunk_path)
    files = [
        f for f in p.glob('*.mp3')
        if f.is_file() and _CHUNK_SEG_RE.match(f.name)
    ]

    def sort_key(f: Path):
        m = _CHUNK_SEG_RE.match(f.name)
        # safe to unwrap because we filtered above
        chunk_num, seg_num = map(int, m.groups())
        return (chunk_num, seg_num)

    files.sort(key=sort_key)
    return [str(f) for f in files]

def run_whisper():
    for path in get_chunk_segments("./chunks"):
        file_to_speaker(path)

if __name__ == "__main__":
    # chunk()
    run_whisper()
