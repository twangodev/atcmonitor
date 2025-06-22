from faster_whisper import WhisperModel
from transformers import pipeline
from typing import Dict, List, Tuple, Union


def file_to_speaker(audio_path: str) -> Union[Dict[str, object], None]:
    """
    Transcribe an audio file and classify its speaker role.

    Returns a dict with:
      - "values": List of (timestamp_seconds, word) tuples
      - "category": "Pilot" or "Tower"
    """
    # 1) Load your Whisper model
    asr = WhisperModel(
        "jacktol/whisper-medium.en-fine-tuned-for-ATC-faster-whisper",
        device="cuda",
        compute_type="float32",
    )

    # 2) Transcribe with word-level timestamps
    segments, _ = asr.transcribe(
        audio_path,
        language="en",
        condition_on_previous_text=True,
        beam_size=10,
        # no_speech_threshold=0.1,
        log_prob_threshold=-0.5,
        temperature=[0.0],
        chunk_length=60,
        vad_filter=True,
        word_timestamps=True,
    )

    # 3) Flatten out (timestamp, word) pairs
    values: List[Tuple[float, str]] = []
    for seg in segments:
        for w in seg.words:
            values.append((w.start, w.word))

    # 4) Build the full transcript for classification
    # Use recognized words instead of seg.text to ensure transcript is not empty
    full_text = " ".join(tup[1].strip() for tup in values if tup[1].strip())

    if not full_text:
        return None


    # 5) Load and run your speaker-role classifier
    role_clf = pipeline(
        "text-classification",
        model="jacktol/atc-pilot-speaker-role-classification-model",
        tokenizer="jacktol/atc-pilot-speaker-role-classification-model",
    )
    pred = role_clf(full_text)[0]
    raw_lbl = pred["label"]

    # print(f"[DEBUG] Raw label from classifier: {raw_lbl}")

    # 6) Map to human-friendly category
    label_map = {
        "PILOT":   "Pilot",
        "ATC":     "Tower",
        "LABEL_0": "Tower",   # fallback if generic
        "LABEL_1": "Pilot",
    }
    category = label_map.get(raw_lbl.upper(), raw_lbl)

    # print(f"[{audio_path}] [{category}]: {full_text}")
    print(f"[{category}]: {full_text}")
    # 7) Return the results
    return {
        "values": values,
        "category": category,
    }