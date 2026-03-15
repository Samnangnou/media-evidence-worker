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
        output_template = os.path.join(tmpdir, "%(id)s.%(ext)s")
        cmd = [
            "yt-dlp",
            "--no-update",
            "--skip-download",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs",
            "en-orig,en.*,en",
            "--sub-format",
            "vtt",
            "--extractor-args",
            "youtube:player_client=web",
            "--extractor-args",
            f"youtubepot-bgutilhttp:base_url={os.environ.get('BGUTIL_BASE_URL', 'http://127.0.0.1:4416')}",
            "-o",
            output_template,
            url,
        ]
        result = run(cmd)
        if result.returncode != 0:
            return None, f"yt-dlp failed: {result.stderr.strip() or result.stdout.strip()}"

        subtitle_files = sorted(Path(tmpdir).glob("*.vtt"))
        if not subtitle_files:
            return None, "No VTT subtitles were produced by yt-dlp."

        for subtitle_path in subtitle_files:
            transcript = read_vtt_as_text(subtitle_path)
            if transcript:
                return transcript, None

        return None, "Subtitle files were present but no transcript text was extracted."


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

    if "subtitles" in operations:
        transcript, error_message = extract_subtitles(payload["canonical_url"])
        if transcript:
            completed.append("subtitles")
        else:
            failed.append("subtitles")

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
            "transcript_source": "yt_dlp_subtitles" if transcript else None,
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
