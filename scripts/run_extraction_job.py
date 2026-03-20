#!/usr/bin/env python3
from __future__ import annotations
import argparse
import base64
import http.cookiejar
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests


TRANSCRIPT_OPS = {"subtitles", "audio_transcript"}
LINKED_PAGE_OPS = {"linked_pages"}
FRAME_OPS = {"keyframes", "ocr"}
VISION_OPS = {"vision"}
UNSUPPORTED_OPS: set[str] = set()
DEFAULT_FRAME_COUNT = 3
DEFAULT_FRAME_FPS = "fps=1/20,scale=480:-1"
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def extract_urls_from_text(value: str | None) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    for match in re.findall(r"https?://[^\s<>()\"']+", str(value or "")):
        candidate = match.rstrip(".,;:!?)")
        if candidate and candidate not in seen:
            seen.add(candidate)
            urls.append(candidate)
    return urls


class LinkExtractor(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        absolute = urljoin(self.base_url, href.strip())
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            return
        self.links.append(absolute)


def read_vtt_as_text(path: Path) -> str:
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line == "WEBVTT":
            continue
        if re.match(r"^\d+$", line):
            continue
        if "-->" in line or line.startswith("NOTE"):
            continue
        line = re.sub(r"<[^>]+>", "", line)
        line = re.sub(r"\[[^\]]+\]", "", line).strip()
        if line:
            lines.append(line)

    seen: set[str] = set()
    deduped: list[str] = []
    for line in lines:
        if line in seen:
            continue
        seen.add(line)
        deduped.append(line)
    return " ".join(deduped).strip()


def parse_youtube_transcript_json(payload: Any) -> str | None:
    events = payload.get("events") if isinstance(payload, dict) else []
    parts: list[str] = []
    for event in events or []:
        segs = event.get("segs") if isinstance(event, dict) else []
        for seg in segs or []:
            text = normalize_text((seg or {}).get("utf8"))
            if text:
                parts.append(text)
    text = normalize_text(" ".join(parts))
    return text or None


def parse_youtube_transcript_xml(payload: str) -> str | None:
    parts = [
        normalize_text(match.group(1))
        for match in re.finditer(r"<text[^>]*>([\s\S]*?)</text>", str(payload or ""), re.I)
    ]
    text = normalize_text(" ".join(part for part in parts if part))
    return text or None


def extract_video_id(url: str) -> str | None:
    match = re.search(r"(?:v=|youtu\.be/|/shorts/)([A-Za-z0-9_-]{6,})", url)
    return match.group(1) if match else None


def extract_transcript_api(url: str) -> tuple[str | None, str | None]:
    video_id = extract_video_id(url)
    if not video_id:
        return None, "Unable to determine video ID for youtube-transcript-api."
    try:
        from youtube_transcript_api import YouTubeTranscriptApi

        snippets = YouTubeTranscriptApi().fetch(video_id, languages=["en"])
        text = " ".join(
            getattr(item, "text", "").strip()
            for item in snippets
            if getattr(item, "text", "").strip()
        ).strip()
        return (text or None), None if text else "youtube-transcript-api returned no transcript text."
    except Exception as error:  # pragma: no cover - network/provider behavior
        return None, str(error)


def build_cookie_session() -> requests.Session:
    session = requests.Session()
    cookies_file = os.environ.get("YOUTUBE_COOKIES_FILE", "").strip()
    if cookies_file and os.path.exists(cookies_file):
        jar = http.cookiejar.MozillaCookieJar()
        jar.load(cookies_file, ignore_discard=True, ignore_expires=True)
        session.cookies.update(jar)
    return session


def extract_watch_page_transcript(url: str) -> tuple[str | None, str | None]:
    session = build_cookie_session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.youtube.com/",
    }
    try:
        response = session.get(url, headers=headers, timeout=30)
        if not response.ok:
            return None, f"watch page fetch failed with HTTP {response.status_code}"

        match = re.search(r"ytInitialPlayerResponse\s*=\s*(\{[\s\S]*?\});", response.text)
        if not match:
            return None, "ytInitialPlayerResponse not found in watch page HTML."

        player = json.loads(match.group(1))
        caption_tracks = (((player or {}).get("captions") or {}).get("playerCaptionsTracklistRenderer") or {}).get("captionTracks") or []
        if not caption_tracks:
            return None, "No caption tracks found in watch page player response."

        preferred = (
            next((track for track in caption_tracks if re.match(r"^en(?:-|$)", str(track.get("languageCode", ""))) and not track.get("kind")), None)
            or next((track for track in caption_tracks if re.match(r"^en(?:-|$)", str(track.get("languageCode", "")))), None)
            or next((track for track in caption_tracks if not track.get("kind")), None)
            or caption_tracks[0]
        )
        base_url = str(preferred.get("baseUrl", "")).strip()
        if not base_url:
            return None, "Preferred caption track did not include a baseUrl."

        transcript_url = f"{base_url}&fmt=json3" if "?" in base_url else f"{base_url}?fmt=json3"
        transcript_response = session.get(transcript_url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.youtube.com/"}, timeout=30)
        if not transcript_response.ok:
            return None, f"timedtext fetch failed with HTTP {transcript_response.status_code}"

        raw = transcript_response.text.strip()
        if not raw:
            return None, "timedtext response was empty."

        transcript = None
        try:
            transcript = parse_youtube_transcript_json(json.loads(raw))
        except Exception:
            transcript = parse_youtube_transcript_xml(raw)

        return (transcript or None), None if transcript else "timedtext payload did not contain transcript text."
    except Exception as error:  # pragma: no cover - network/provider behavior
        return None, str(error)
    finally:
        session.close()


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

        errors: list[str] = []
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

            combined = "\n".join(
                part for part in [result.stderr.strip(), result.stdout.strip()] if part
            ).strip()
            errors.append(f"[{strategy['name']}] {combined or 'No subtitles produced.'}")

        transcript, transcript_error = extract_transcript_api(url)
        if transcript:
            return transcript, None
        if transcript_error:
            errors.append(f"[youtube_transcript_api] {transcript_error}")

        transcript, watch_page_error = extract_watch_page_transcript(url)
        if transcript:
            return transcript, None
        if watch_page_error:
            errors.append(f"[watch_page] {watch_page_error}")

        return None, "yt-dlp failed across strategies:\n" + "\n\n".join(errors)


def youtube_ytdlp_strategies() -> list[dict[str, Any]]:
    return [
        {
            "name": "android_audio",
            "args": [
                "--extractor-args",
                "youtube:player_client=android",
            ],
        },
        {
            "name": "android_tv_audio",
            "args": [
                "--extractor-args",
                "youtube:player_client=tv_embedded,android",
            ],
        },
        {
            "name": "web_bgutil_audio",
            "args": [
                "--extractor-args",
                "youtube:player_client=web",
                "--extractor-args",
                f"youtubepot-bgutilhttp:base_url={os.environ.get('BGUTIL_BASE_URL', 'http://127.0.0.1:4416')}",
            ],
        },
    ]


def transcribe_audio_file(input_source: str, tmpdir: str) -> tuple[str | None, str | None]:
    audio_path = os.path.join(tmpdir, "audio.wav")
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_source,
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
        combined = "\n".join(
            part for part in [ffmpeg_result.stderr.strip(), ffmpeg_result.stdout.strip()] if part
        ).strip()
        return None, f"ffmpeg failed: {combined}"

    try:
        from faster_whisper import WhisperModel

        model = WhisperModel("tiny.en", device="cpu", compute_type="int8")
        segments, _info = model.transcribe(audio_path, vad_filter=True, beam_size=1)
        text = " ".join(segment.text.strip() for segment in segments if segment.text.strip()).strip()
        return (text or None), None if text else "faster-whisper returned no transcript text."
    except Exception as error:  # pragma: no cover - model/runtime behavior
        return None, f"faster-whisper failed: {error}"


def download_youtube_media_for_audio(url: str, tmpdir: str) -> tuple[str | None, str | None]:
    cookies_file = os.environ.get("YOUTUBE_COOKIES_FILE", "").strip()
    errors: list[str] = []

    for strategy in youtube_ytdlp_strategies():
        strategy_dir = os.path.join(tmpdir, strategy["name"])
        os.makedirs(strategy_dir, exist_ok=True)
        output_template = os.path.join(strategy_dir, "%(id)s.%(ext)s")
        cmd = [
            "yt-dlp",
            "-v",
            "--no-update",
            *(["--cookies", cookies_file] if cookies_file else []),
            "--no-playlist",
            "-f",
            "bestaudio/best",
            *strategy["args"],
            "-o",
            output_template,
            url,
        ]
        result = run(cmd)
        media_files = [
            path for path in Path(strategy_dir).iterdir()
            if path.is_file() and path.suffix.lower() not in {".vtt", ".json", ".part", ".ytdl"}
        ]
        if media_files:
            return str(media_files[0]), None

        combined = "\n".join(
            part for part in [result.stderr.strip(), result.stdout.strip()] if part
        ).strip()
        errors.append(f"[{strategy['name']}] {combined or 'No media downloaded.'}")

    return None, "yt-dlp audio download failed across strategies:\n" + "\n\n".join(errors)


def extract_audio_transcript(canonical_url: str, video_url: str | None = None) -> tuple[str | None, str | None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        downloaded_media, download_error = download_youtube_media_for_audio(canonical_url, tmpdir)
        if downloaded_media:
            transcript, transcript_error = transcribe_audio_file(downloaded_media, tmpdir)
            if transcript:
                return transcript, None
            return None, transcript_error

        if video_url:
            transcript, transcript_error = transcribe_audio_file(video_url, tmpdir)
            if transcript:
                return transcript, None
            combined_errors = [download_error, f"[direct_media] {transcript_error}" if transcript_error else None]
            return None, "\n\n".join(part for part in combined_errors if part)

        return None, download_error or "No usable media source was available."


def fetch_linked_pages(canonical_url: str, metadata: dict[str, Any] | None) -> tuple[list[str], str | None]:
    urls = extract_urls_from_text(((metadata or {}).get("youtube_context") or {}).get("description"))
    request_kwargs = {
        "headers": {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml",
        },
        "timeout": 30,
    }
    try:
        response = requests.get(canonical_url, **request_kwargs)
    except requests.exceptions.SSLError:
        response = requests.get(canonical_url, verify=False, **request_kwargs)
    except Exception as error:  # pragma: no cover - network/provider behavior
        if urls:
            return dedupe_urls(urls), str(error)
        return [], str(error)

    if response.ok and response.text.strip():
        parser = LinkExtractor(canonical_url)
        parser.feed(response.text)
        urls.extend(parser.links)
    elif response.status_code >= 400:
        return dedupe_urls(urls), f"linked page fetch failed with HTTP {response.status_code}"

    return dedupe_urls(urls), None


def dedupe_urls(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        candidate = normalize_text(value)
        if not candidate:
            continue
        parsed = urlparse(candidate)
        if parsed.scheme not in {"http", "https"}:
            continue
        normalized = candidate.rstrip("/")
        if normalized in seen:
            continue
        seen.add(normalized)
        output.append(candidate)
    return output


def resolve_video_url(payload: dict[str, Any]) -> str | None:
    youtube_context = ((payload.get("metadata") or {}).get("youtube_context") or {})
    for candidate in [
        youtube_context.get("videoUrl"),
        youtube_context.get("video_url"),
        payload.get("canonical_url"),
    ]:
        value = normalize_text(candidate)
        if value.startswith("http://") or value.startswith("https://"):
            return value
    return None


def is_image_url(url: str | None) -> bool:
    candidate = normalize_text(url)
    if not candidate.startswith("http://") and not candidate.startswith("https://"):
        return False
    parsed = urlparse(candidate)
    return Path(parsed.path).suffix.lower() in IMAGE_EXTENSIONS


def download_remote_asset(url: str, suffix: str | None = None) -> tuple[Path | None, str | None]:
    request_kwargs = {
        "headers": {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        },
        "timeout": 30,
        "stream": True,
    }
    try:
        response = requests.get(url, **request_kwargs)
    except requests.exceptions.SSLError:
        response = requests.get(url, verify=False, **request_kwargs)
    except Exception as error:  # pragma: no cover - network/provider behavior
        return None, str(error)

    if not response.ok:
        return None, f"asset fetch failed with HTTP {response.status_code}"

    parsed = urlparse(url)
    ext = suffix or Path(parsed.path).suffix.lower() or ".bin"
    tmpdir = tempfile.mkdtemp()
    path = Path(tmpdir) / f"asset{ext}"
    with path.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=65536):
            if chunk:
                fh.write(chunk)
    return path, None


def build_image_artifact(path: Path) -> dict[str, Any]:
    data = path.read_bytes()
    encoded = base64.b64encode(data).decode("ascii")
    mime = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".bmp": "image/bmp",
        ".tif": "image/tiff",
        ".tiff": "image/tiff",
    }.get(path.suffix.lower(), "application/octet-stream")
    return {
        "kind": "image",
        "url": f"data:{mime};base64,{encoded}",
        "timestamp_ms": 0,
    }


def extract_keyframes(video_url: str) -> tuple[list[dict[str, Any]], list[Path], str | None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_pattern = os.path.join(tmpdir, "frame-%03d.jpg")
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            video_url,
            "-vf",
            DEFAULT_FRAME_FPS,
            "-frames:v",
            str(DEFAULT_FRAME_COUNT),
            output_pattern,
        ]
        result = run(cmd)
        if result.returncode != 0:
            combined = "\n".join(part for part in [result.stderr.strip(), result.stdout.strip()] if part).strip()
            return [], [], f"ffmpeg keyframe extraction failed: {combined}"

        frame_paths = sorted(Path(tmpdir).glob("frame-*.jpg"))
        artifacts: list[dict[str, Any]] = []
        persisted_paths: list[Path] = []
        for index, frame_path in enumerate(frame_paths):
            data = frame_path.read_bytes()
            if not data:
                continue
            encoded = base64.b64encode(data).decode("ascii")
            artifacts.append({
                "kind": "image",
                "url": f"data:image/jpeg;base64,{encoded}",
                "timestamp_ms": index * 20_000,
            })
            persisted_path = Path(tempfile.gettempdir()) / f"{frame_path.stem}-{os.getpid()}.jpg"
            persisted_path.write_bytes(data)
            persisted_paths.append(persisted_path)

        if artifacts:
            return artifacts, persisted_paths, None
        return [], [], "No frame artifacts were produced."


def extract_ocr_text(frame_paths: list[Path]) -> tuple[str | None, str | None]:
    collected: list[str] = []
    errors: list[str] = []
    for frame_path in frame_paths:
        result = run([
            "tesseract",
            str(frame_path),
            "stdout",
            "--psm",
            "6",
        ])
        if result.returncode != 0:
            combined = "\n".join(part for part in [result.stderr.strip(), result.stdout.strip()] if part).strip()
            errors.append(combined or f"tesseract failed for {frame_path.name}")
            continue
        text = normalize_text(result.stdout)
        if text:
            collected.append(text)

    for frame_path in frame_paths:
        frame_path.unlink(missing_ok=True)

    if collected:
        return normalize_text(" ".join(collected)) or None, None
    if errors:
        return None, "; ".join(errors)
    return None, "No OCR text extracted."


def summarize_visual_semantics(frame_artifacts: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    nvidia_key = os.environ.get("NVIDIA_API_KEY", "").strip()
    if not nvidia_key:
        return None, "NVIDIA_API_KEY is not configured for vision summarization."
    image_urls = []
    for artifact in frame_artifacts[:3]:
        url = normalize_text((artifact or {}).get("url"))
        if url.startswith("data:image/"):
            image_urls.append(url)
    if not image_urls:
        return None, "No frame artifacts were available for vision summarization."

    summaries: list[str] = []
    errors: list[str] = []

    for image_url in image_urls:
        body = {
            "model": "meta/llama-3.2-11b-vision-instruct",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Describe only what is visibly shown in this image in 1 to 2 short factual sentences. Mention clearly visible text if present. Do not speculate.",
                        },
                        {"type": "image_url", "image_url": {"url": image_url}},
                    ],
                }
            ],
            "max_tokens": 160,
            "temperature": 0.1,
        }

        try:
            response = requests.post(
                "https://integrate.api.nvidia.com/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {nvidia_key}",
                },
                data=json.dumps(body),
                timeout=60,
            )
            if not response.ok:
                errors.append(f"HTTP {response.status_code}: {response.text[:200]}")
                continue
            data = response.json()
            content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content"))
            summary = normalize_text(content if isinstance(content, str) else "")
            if summary:
                summaries.append(summary)
        except Exception as error:  # pragma: no cover - provider behavior
            errors.append(str(error))

    deduped: list[str] = []
    seen: set[str] = set()
    for summary in summaries:
        key = summary.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(summary)

    if deduped:
        return normalize_text(" ".join(deduped[:2])) or None, None
    if errors:
        return None, f"NVIDIA vision request failed: {'; '.join(errors)}"
    return None, "NVIDIA vision returned no summary text."


def determine_status(completed: list[str], failed: list[str]) -> str:
    if completed and failed:
        return "partial"
    if completed:
        return "success"
    return "failed"


@dataclass
class JobResult:
    callback_payload: dict[str, Any]
    transcript_found: bool
    error_message: str | None
    debug: dict[str, Any]


def execute_job(payload: dict[str, Any]) -> JobResult:
    callback_url = payload["callback_url"]
    callback_signature = payload["callback_signature"]
    operations = set(payload.get("operations", []))

    transcript: str | None = None
    transcript_source: str | None = None
    linked_urls: list[str] = []
    ocr_text: str | None = None
    vision_summary: str | None = None
    frame_artifacts: list[dict[str, Any]] = []
    error_messages: list[str] = []
    completed: list[str] = []
    failed: list[str] = []
    debug: dict[str, Any] = {"operations_requested": sorted(operations)}

    frame_paths: list[Path] = []

    if "subtitles" in operations:
        transcript, subtitle_error = extract_subtitles(payload["canonical_url"])
        if transcript:
            completed.append("subtitles")
            transcript_source = "yt_dlp_subtitles"
        else:
            failed.append("subtitles")
            if subtitle_error:
                error_messages.append(f"[subtitles] {subtitle_error}")

    if not transcript and "audio_transcript" in operations:
        video_url = resolve_video_url(payload)
        debug["audio_video_url_available"] = bool(video_url)
        transcript, audio_error = extract_audio_transcript(payload["canonical_url"], video_url)
        if transcript:
            completed.append("audio_transcript")
            transcript_source = "faster_whisper_audio"
        else:
            failed.append("audio_transcript")
            if audio_error:
                error_messages.append(f"[audio_transcript] {audio_error}")

    if LINKED_PAGE_OPS & operations:
        linked_urls, link_error = fetch_linked_pages(payload["canonical_url"], payload.get("metadata") or {})
        if linked_urls:
            completed.append("linked_pages")
        else:
            failed.append("linked_pages")
        if link_error:
            error_messages.append(f"[linked_pages] {link_error}")
        debug["linked_url_count"] = len(linked_urls)

    if FRAME_OPS & operations:
        video_url = resolve_video_url(payload)
        debug["frame_video_url_available"] = bool(video_url)
        canonical_url = normalize_text(payload.get("canonical_url"))
        if is_image_url(canonical_url):
            image_path, image_error = download_remote_asset(canonical_url)
            if image_path:
                frame_artifacts = [build_image_artifact(image_path)]
                frame_paths = [image_path]
                if "keyframes" in operations:
                    completed.append("keyframes")
            else:
                if "keyframes" in operations:
                    failed.append("keyframes")
                    error_messages.append(f"[keyframes] {image_error or 'No image asset available.'}")
                if "ocr" in operations:
                    failed.append("ocr")
                    error_messages.append(f"[ocr] {image_error or 'No image asset available.'}")
        elif video_url:
            frame_artifacts, frame_paths, frame_error = extract_keyframes(video_url)
            if frame_artifacts:
                completed.append("keyframes")
            elif "keyframes" in operations:
                failed.append("keyframes")
            if frame_error and "keyframes" in operations:
                error_messages.append(f"[keyframes] {frame_error}")
        elif "keyframes" in operations or "ocr" in operations:
            if "keyframes" in operations:
                failed.append("keyframes")
                error_messages.append("[keyframes] No usable video URL was available.")

        if "ocr" in operations:
            if frame_paths:
                ocr_text, ocr_error = extract_ocr_text(frame_paths)
                if ocr_text:
                    completed.append("ocr")
                else:
                    failed.append("ocr")
                if ocr_error:
                    error_messages.append(f"[ocr] {ocr_error}")
            else:
                failed.append("ocr")
                error_messages.append("[ocr] No frame artifacts were available for OCR.")
        elif frame_paths:
            for frame_path in frame_paths:
                frame_path.unlink(missing_ok=True)

    if VISION_OPS & operations:
        if frame_artifacts:
            vision_summary, vision_error = summarize_visual_semantics(frame_artifacts)
            if vision_summary:
                completed.append("vision")
            else:
                failed.append("vision")
                if vision_error:
                    error_messages.append(f"[vision] {vision_error}")
        else:
            failed.append("vision")
            error_messages.append("[vision] No frame artifacts were available for vision summarization.")

    if UNSUPPORTED_OPS & operations:
        for operation in sorted(UNSUPPORTED_OPS & operations):
            failed.append(operation)
            error_messages.append(f"[{operation}] Not implemented in the public extraction worker. This remains a downstream AI-plane responsibility.")

    callback_payload = {
        "schema_version": "v1",
        "dispatch_id": payload["dispatch_id"],
        "candidate_id": payload["candidate_id"],
        "candidate_key": payload["candidate_key"],
        "status": determine_status(completed, failed),
        "operations_completed": completed,
        "operations_failed": failed,
        "evidence_updates": {
            "transcript": transcript,
            "transcript_source": transcript_source if transcript else None,
            "linked_urls": linked_urls or [],
            "ocr_text": ocr_text,
            "vision_summary": vision_summary,
            "frame_artifacts": frame_artifacts,
        },
        "debug": debug,
        "error_message": "\n\n".join(error_messages).strip() or None,
    }

    debug["callback_url"] = callback_url
    debug["callback_signature_present"] = bool(callback_signature)
    debug["completed_count"] = len(completed)
    debug["failed_count"] = len(failed)

    return JobResult(
        callback_payload=callback_payload,
        transcript_found=bool(transcript),
        error_message=callback_payload["error_message"],
        debug=debug,
    )


def post_callback(callback_url: str, callback_signature: str, callback_payload: dict[str, Any]) -> requests.Response:
    return requests.post(
        callback_url,
        headers={
            "Content-Type": "application/json",
            "x-public-news-signature": callback_signature,
        },
        data=json.dumps(callback_payload),
        timeout=60,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload-json", required=True)
    args = parser.parse_args()

    payload = json.loads(args.payload_json)
    result = execute_job(payload)

    response = post_callback(
        payload["callback_url"],
        payload["callback_signature"],
        result.callback_payload,
    )

    print(json.dumps({
        "callback_status": response.status_code,
        "callback_body": response.text[:1000],
        "status": result.callback_payload["status"],
        "transcript_found": result.transcript_found,
        "operations_completed": result.callback_payload["operations_completed"],
        "operations_failed": result.callback_payload["operations_failed"],
        "error_message": result.error_message,
    }, ensure_ascii=False))

    if response.status_code >= 400:
        sys.exit(1)


if __name__ == "__main__":
    main()
