import os
import webrtcvad
import torchaudio
import numpy as np
from pydub import AudioSegment
from speechbrain.pretrained import EncoderClassifier
from sklearn.metrics.pairwise import cosine_distances
from tqdm import tqdm

# ─── Configuration ─────────────────────────────────────────────────────────────
INPUT_PATH   = "C:/Users/bryan/Downloads/sfo-air-canada.mp3"
OUT_DIR      = "chunks"
VAD_MODE     = 3    # 0–3 aggression for static rejection
FRAME_MS     = 20     # VAD frame size in ms (must be 10,20 or 30)
PAD_MS       = 300    # pad each VAD segment by 300 ms
WIN_SEC      = 1.0    # speaker‐segmentation window length (s)
HOP_SEC      = 0.5    # speaker‐segmentation hop (s)
THRESHOLD    = 0.5    # cosine‐distance threshold for change points
DEVICE       = "cuda"  # or "cuda"

os.makedirs(OUT_DIR, exist_ok=True)


# ─── Step 1: Read & VAD‐chunk MP3 ───────────────────────────────────────────────
def read_audio(path):
    audio = AudioSegment.from_file(path, format="mp3") \
        .set_channels(1).set_frame_rate(16000)
    return audio, audio.raw_data, audio.frame_rate

def frame_generator(frame_ms, pcm_bytes, sample_rate):
    bytes_per_frame = int(sample_rate * (frame_ms/1000.0) * 2)
    offset = 0
    while offset + bytes_per_frame <= len(pcm_bytes):
        yield pcm_bytes[offset:offset+bytes_per_frame]
        offset += bytes_per_frame

def vad_segments(pcm_bytes, sample_rate, frame_ms=FRAME_MS, pad_ms=PAD_MS):
    vad = webrtcvad.Vad(VAD_MODE)
    frames = list(frame_generator(frame_ms, pcm_bytes, sample_rate))
    segments, voiced = [], []
    for i, frame in enumerate(frames):
        if vad.is_speech(frame, sample_rate):
            voiced.append(i)
        elif voiced:
            start = max(voiced[0]*frame_ms - pad_ms, 0)
            end   = (voiced[-1]+1)*frame_ms + pad_ms
            segments.append((start, end))
            voiced = []
    if voiced:
        start = max(voiced[0]*frame_ms - pad_ms, 0)
        end   = len(frames)*frame_ms
        segments.append((start, end))
    return segments

def export_vad_chunks(audio, segments):
    """
    Export each VAD segment as chunk_{i}.mp3 and return list of paths.
    """
    paths = []
    for idx, (start, end) in enumerate(segments):
        chunk = audio[start:end]
        out_path = os.path.join(OUT_DIR, f"chunk_{idx}.mp3")
        chunk.export(out_path, format="mp3")
        paths.append(out_path)
    return paths


# ─── Step 2: Speaker‐segmentation per chunk ────────────────────────────────────
def load_waveform(path):
    """Load MP3 via torchaudio (mono, 16 kHz)"""
    wav, sr = torchaudio.load(path)
    if wav.size(0) > 1:
        wav = wav.mean(dim=0, keepdim=True)
    return wav, sr

def detect_change_points(wav, sr, window_sec, hop_sec, threshold):
    """
    Returns list of change‐point times (in seconds) where cosine‐distance > threshold.
    Handles short segments by embedding entire chunk if shorter than window.
    """
    # Initialize speaker embedding model
    classifier = EncoderClassifier.from_hparams(
        source="speechbrain/spkrec-ecapa-voxceleb",
        run_opts={"device": DEVICE}
    )

    window_size = int(window_sec * sr)
    hop_size    = int(hop_sec    * sr)
    total_frames = wav.size(1)

    embeddings = []
    times = []

    # Generate embeddings for each window (or entire chunk if short)
    if total_frames < window_size:
        emb_tensor = classifier.encode_batch(wav)
        emb_np = emb_tensor.squeeze().detach().cpu().numpy().ravel()
        embeddings.append(emb_np)
        times.append(0.0)
    else:
        for start in range(0, total_frames - window_size + 1, hop_size):
            segment = wav[:, start:start + window_size]
            emb_tensor = classifier.encode_batch(segment)
            emb_np = emb_tensor.squeeze().detach().cpu().numpy().ravel()
            embeddings.append(emb_np)
            times.append(start / sr)

    # No embeddings or single embedding, no change points
    if len(embeddings) < 2:
        return []

    # Stack embeddings into 2D array [num_windows, dim]
    embs = np.stack(embeddings)

    # Compute cosine distances between successive embeddings
    dists = cosine_distances(embs[:-1], embs[1:]).diagonal()
    return [times[i+1] for i, d in enumerate(dists) if d > threshold]





def export_speaker_subchunks(chunk_path, change_points):
    """
    Given chunk_{i}.mp3 and a list of change times,
    slice and export subchunks chunk_{i}_seg_{j}.mp3
    """
    audio = AudioSegment.from_file(chunk_path, format="mp3")
    duration_ms = len(audio)
    boundaries = [0] + [int(cp*1000) for cp in change_points] + [duration_ms]
    base = os.path.splitext(os.path.basename(chunk_path))[0]

    for j in tqdm(range(len(boundaries)-1), desc=f"Exporting segments for {base}", unit="seg"):
        start_ms = boundaries[j]
        end_ms   = boundaries[j+1]
        sub = audio[start_ms:end_ms]
        out_name = f"{base}_seg_{j}.mp3"
        sub.export(os.path.join(OUT_DIR, out_name), format="mp3")




def chunk():
    audio, pcm, sr = read_audio(INPUT_PATH)
    vad_segments_list = vad_segments(pcm, sr)
    chunks = export_vad_chunks(audio, vad_segments_list)

    for chunk_file in tqdm(chunks, desc="Processing chunks", unit="file"):
        wav, sr = load_waveform(chunk_file)
        cps = detect_change_points(wav, sr, WIN_SEC, HOP_SEC, THRESHOLD)
        export_speaker_subchunks(chunk_file, cps)

    print("Done! All VAD+speaker segments are in", OUT_DIR)


if __name__ == "__main__":
    chunk()