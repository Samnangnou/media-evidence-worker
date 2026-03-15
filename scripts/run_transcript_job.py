#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import requests
from faster_whisper import WhisperModel
from youtube_transcript_api import YouTubeTranscriptApi


def run(cmd):
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def read_vtt_as_text(path: Path) -> str:
    lines = []
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line == "WEBVTT":
            continue
        if re.match(r"^\d+$", line):
            continue
        if "-->" in line:
            continue
        if line.startswith("NOTE"):
            continue
        line = re.sub(r"<[^>]+>", "", line)
        line = re.sub(r"\[[^\]]+\]", "", line).strip()
        if not line:
            continue
        lines.append(line)

    seen = set()
    deduped = []
    for line in lines:
      if line in seen:
        continue
      seen.add(line)
      deduped.append(line)
    return " ".join(deduped).strip()


def extract_subtitles(url: str) -> tuple[str | None, str | None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        cookies_file = os.environ.get("YOUTUBE_COOKIES_FILE", "").strip()
        strategies = [
            {
                "name": "android_auto",
                "args": [
                    "--extractor-args",
                    "youtube:player_client=android",
                ],
            },
            {
                "name": "android_tv_combo",
                "args": [
                    "--extractor-args",
                    "youtube:player_client=tv_embedded,android",
                ],
            },
            {
                "name": "web_bgutil",
                "args": [
                    "--extractor-args",
                    "youtube:player_client=web",
                    "--extractor-args",
                    f"youtubepot-bgutilhttp:base_url={os.environ.get('BGUTIL_BASE_URL', 'http://127.0.0.1:4416')}",
                ],
            },
        ]

        errors = []
        for strategy in strategies:
            strategy_dir = os.path.join(tmpdir, strategy["name"])
            os.makedirs(strategy_dir, exist_ok=True)
            output_template = os.path.join(strategy_dir, "%(id)s.%(ext)s")
            cmd = [
                "yt-dlp",
                "-v",
                "--no-update",
                *(["--cookies", cookies_file] if cookies_file else []),
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",
                "--sub-langs",
                "en-orig,en.*,en",
                "--sub-format",
                "vtt",
                *strategy["args"],
                "-o",
                output_template,
                url,
            ]
            result = run(cmd)
            subtitle_files = sorted(Path(strategy_dir).glob("*.vtt"))
            for subtitle_path in subtitle_files:
                transcript = read_vtt_as_text(subtitle_path)
                if transcript:
                    return transcript, None

            combined = "\n".join(part for part in [result.stderr.strip(), result.stdout.strip()] if part).strip()
            errors.append(f"[{strategy['name']}] {combined or 'No subtitles produced.'}")

        transcript, transcript_error = extract_transcript_api(url)
        if transcript:
            return transcript, None
        if transcript_error:
            errors.append(f"[youtube_transcript_api] {transcript_error}")

        return None, "yt-dlp failed across strategies:\n" + "\n\n".join(errors)


def extract_video_id(url: str) -> str | None:
    match = re.search(r"(?:v=|youtu\\.be/|/shorts/)([A-Za-z0-9_-]{6,})", url)
    return match.group(1) if match else None


def extract_transcript_api(url: str) -> tuple[str | None, str | None]:
    video_id = extract_video_id(url)
    if not video_id:
        return None, "Unable to determine video ID for youtube-transcript-api."
    try:
        snippets = YouTubeTranscriptApi().fetch(video_id, languages=["en"])
        text = " ".join(getattr(item, "text", "").strip() for item in snippets if getattr(item, "text", "").strip()).strip()
        return (text or None), None if text else "youtube-transcript-api returned no transcript text."
    except Exception as error:
        return None, str(error)


def extract_audio_transcript(video_url: str) -> tuple[str | None, str | None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = os.path.join(tmpdir, "audio.wav")
        ffmpeg_cmd = [
            "ffmpeg",
            "-y",
            "-i",
            video_url,
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-f",
            "wav",
            audio_path,
        ]
        ffmpeg_result = run(ffmpeg_cmd)
        if ffmpeg_result.returncode != 0:
            combined = "\n".join(part for part in [ffmpeg_result.stderr.strip(), ffmpeg_result.stdout.strip()] if part).strip()
            return None, f"ffmpeg failed: {combined}"

        try:
            model = WhisperModel("tiny.en", device="cpu", compute_type="int8")
            segments, _info = model.transcribe(audio_path, vad_filter=True, beam_size=1)
            text = " ".join(segment.text.strip() for segment in segments if segment.text.strip()).strip()
            return (text or None), None if text else "faster-whisper returned no transcript text."
        except Exception as error:
            return None, f"faster-whisper failed: {error}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload-json", required=True)
    args = parser.parse_args()

    payload = json.loads(args.payload_json)
    callback_url = payload["callback_url"]
    callback_signature = payload["callback_signature"]
    operations = set(payload.get("operations", []))

    transcript = None
    error_message = None
    completed = []
    failed = []
    transcript_source = None

    if "subtitles" in operations:
        transcript, error_message = extract_subtitles(payload["canonical_url"])
        if transcript:
            completed.append("subtitles")
            transcript_source = "yt_dlp_subtitles"
        else:
            failed.append("subtitles")

    if not transcript and "audio_transcript" in operations:
        video_url = (((payload.get("metadata") or {}).get("youtube_context") or {}).get("videoUrl") or "").strip()
        if video_url:
            transcript, audio_error = extract_audio_transcript(video_url)
            if transcript:
                completed.append("audio_transcript")
                transcript_source = "faster_whisper_audio"
                error_message = None
            else:
                failed.append("audio_transcript")
                error_message = f"{error_message}\n\n[audio_transcript] {audio_error}".strip() if error_message else audio_error
        else:
            failed.append("audio_transcript")
            missing_video_error = "No videoUrl was available for audio transcript fallback."
            error_message = f"{error_message}\n\n[audio_transcript] {missing_video_error}".strip() if error_message else missing_video_error

    callback_payload = {
        "schema_version": "v1",
        "dispatch_id": payload["dispatch_id"],
        "candidate_id": payload["candidate_id"],
        "candidate_key": payload["candidate_key"],
        "status": "success" if transcript else "failed",
        "operations_completed": completed,
        "operations_failed": failed,
        "evidence_updates": {
            "transcript": transcript,
            "transcript_source": transcript_source if transcript else None,
        },
        "error_message": error_message,
    }

    response = requests.post(
        callback_url,
        headers={
            "Content-Type": "application/json",
            "x-public-news-signature": callback_signature,
        },
        data=json.dumps(callback_payload),
        timeout=60,
    )
    print(json.dumps({
        "callback_status": response.status_code,
        "callback_body": response.text[:1000],
        "transcript_found": bool(transcript),
        "error_message": error_message,
    }, ensure_ascii=False))

    if response.status_code >= 400:
        sys.exit(1)


if __name__ == "__main__":
    main()
