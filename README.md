# Media Evidence Worker

Generic async media extraction worker.

This repository is intended for reusable extraction workflows such as:

- transcript extraction
- linked-page extraction
- frame sampling
- OCR preparation
- generic media-processing jobs

It is designed to stay generic and reusable across different applications.

Current workflow entrypoint:

- `.github/workflows/transcript-extract.yml`

Current runner script:

- `scripts/run_extraction_job.py`

Supported extraction operations today:

- `subtitles`
- `audio_transcript`
- `linked_pages`
- `keyframes`
- `ocr`

Current non-goal in this public repo:

- semantic `vision` summarization

That remains a downstream AI-plane responsibility and should not contain private product logic in this repository.
