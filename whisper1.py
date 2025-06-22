import soundfile as sf
import librosa
from faster_whisper import WhisperModel
from typing import BinaryIO, List, Tuple

# This function works with a BinaryIO stream and returns a list of (timestamp, word) tuples.

def audio_to_text(audio_io: BinaryIO) -> List[Tuple[float, str]]:
    """
    Read the entire audio from `audio_io`, run Whisper ASR with word-level
    timestamps, and return a list of (timestamp, word) tuples.
    """
    # 1) rewind and load
    audio_io.seek(0)
    data, orig_sr = sf.read(audio_io, dtype="float32")
    if data.ndim > 1:
        data = data.mean(axis=1)  # to mono

    # 2) resample to 16 kHz (Whisper’s fixed rate)
    TARGET_SR = 16_000
    if orig_sr != TARGET_SR:
        # nylibrosa.resample now takes orig_sr and target_sr as keyword-only
        data = librosa.resample(y=data, orig_sr=orig_sr, target_sr=TARGET_SR)

    # 3) instantiate ASR model locally
    asr = WhisperModel(
        "jacktol/whisper-medium.en-fine-tuned-for-ATC-faster-whisper",
        device="cuda",
        compute_type="float32",
    )

    # 4) transcribe with word timestamps
    segments, _ = asr.transcribe(
        data,
        language="en",
        # condition_on_previous_text=True,
        # beam_size=10,
        # log_prob_threshold=-0.5,
        word_timestamps=True,
    )

    # 5) flatten into (timestamp, word) list
    word_list: List[Tuple[float, str]] = []
    for seg in segments:
        for w in seg.words:
            word_list.append((w.start, w.word))

    return word_list

# test_audio_no_extra.py
import io
import subprocess

def main():
    # mp3_path = r"C:/Users/bryan/Downloads/ATC_audio.mp3"
    # mp3_path = r"C:/Users/bryan/IdeaProjects/atcmon/audio/chunks/chunk_1.mp3"
    mp3_path = "C:/Users/bryan/Documents/Sound Recordings/Trial1.mp3"

    # 1) Spawn ffmpeg to decode MP3 → WAV on stdout (needs ffmpeg on your PATH)
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner", "-loglevel", "error",
        "-i", mp3_path,
        "-f", "wav",
        "-acodec", "pcm_s16le",   # 16-bit PCM; soundfile will cast to float32
        "pipe:1"
    ]
    proc = subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        print("ffmpeg error:", proc.stderr.decode().strip())
        return

    # 2) Wrap the raw WAV bytes in a BytesIO and hand off to your function
    wav_io = io.BytesIO(proc.stdout)
    words = audio_to_text(wav_io)

    # 3) Nicely print each word with its start-time
    for ts, w in words:
        print(f"{ts:6.2f}s │ {w}")

if __name__ == "__main__":
    main()
