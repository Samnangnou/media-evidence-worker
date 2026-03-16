import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import run_extraction_job


class FakeResponse:
    def __init__(self, status_code=200, text="", ok=True):
        self.status_code = status_code
        self.text = text
        self.ok = ok


class RunExtractionJobTests(unittest.TestCase):
    def test_determine_status(self):
        self.assertEqual(run_extraction_job.determine_status(["subtitles"], []), "success")
        self.assertEqual(run_extraction_job.determine_status(["subtitles"], ["vision"]), "partial")
        self.assertEqual(run_extraction_job.determine_status([], ["vision"]), "failed")

    def test_fetch_linked_pages_merges_description_and_html_links(self):
        html = """
        <html><body>
        <a href="https://example.com/report">Report</a>
        <a href="/internal">Internal</a>
        </body></html>
        """

        class Response:
            ok = True
            status_code = 200
            text = html

        metadata = {
            "youtube_context": {
                "description": "See https://openai.com/index/rakuten/ for more."
            }
        }

        with patch.object(run_extraction_job.requests, "get", return_value=Response()):
            urls, error = run_extraction_job.fetch_linked_pages("https://example.com/watch", metadata)

        self.assertIsNone(error)
        self.assertEqual(urls, [
            "https://openai.com/index/rakuten/",
            "https://example.com/report",
            "https://example.com/internal",
        ])

    def test_fetch_linked_pages_retries_without_ssl_verification_on_ssl_error(self):
        html = "<html><body><a href=\"https://example.com/report\">Report</a></body></html>"

        class Response:
            ok = True
            status_code = 200
            text = html

        calls = []

        def fake_get(url, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise run_extraction_job.requests.exceptions.SSLError("ssl failed")
            return Response()

        with patch.object(run_extraction_job.requests, "get", side_effect=fake_get):
            urls, error = run_extraction_job.fetch_linked_pages("https://example.com", {})

        self.assertIsNone(error)
        self.assertEqual(urls, ["https://example.com/report"])
        self.assertNotIn("verify", calls[0])
        self.assertEqual(calls[1]["verify"], False)

    def test_execute_job_builds_partial_callback_for_supported_and_unsupported_ops(self):
        payload = {
            "schema_version": "v1",
            "dispatch_id": "dispatch-1",
            "candidate_id": "candidate-1",
            "candidate_key": "candidate-key-1",
            "canonical_url": "https://www.youtube.com/watch?v=abc123",
            "callback_url": "https://callback.test/extraction",
            "callback_signature": "sig",
            "operations": ["linked_pages", "vision"],
            "metadata": {
                "youtube_context": {
                    "description": "Primary link https://example.com/a"
                }
            },
        }

        with patch.object(run_extraction_job, "fetch_linked_pages", return_value=(["https://example.com/a"], None)):
            result = run_extraction_job.execute_job(payload)

        self.assertEqual(result.callback_payload["status"], "partial")
        self.assertEqual(result.callback_payload["operations_completed"], ["linked_pages"])
        self.assertEqual(result.callback_payload["operations_failed"], ["vision"])
        self.assertEqual(result.callback_payload["evidence_updates"]["linked_urls"], ["https://example.com/a"])
        self.assertIn("Not implemented", result.callback_payload["error_message"])

    def test_extract_ocr_text_aggregates_tesseract_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            frame = Path(tmpdir) / "frame-001.jpg"
            frame.write_bytes(b"fake")

            def fake_run(cmd):
                class Result:
                    returncode = 0
                    stdout = "Hello OCR"
                    stderr = ""
                return Result()

            with patch.object(run_extraction_job, "run", side_effect=fake_run):
                text, error = run_extraction_job.extract_ocr_text([frame])

        self.assertEqual(text, "Hello OCR")
        self.assertIsNone(error)

    def test_post_callback_sends_signed_payload(self):
        payload = {
            "schema_version": "v1",
            "dispatch_id": "dispatch-1",
        }

        with patch.object(run_extraction_job.requests, "post", return_value=FakeResponse(text="ok")) as mock_post:
            response = run_extraction_job.post_callback("https://callback.test", "sig", payload)

        self.assertEqual(response.text, "ok")
        kwargs = mock_post.call_args.kwargs
        self.assertEqual(kwargs["headers"]["x-public-news-signature"], "sig")
        self.assertEqual(json.loads(kwargs["data"]), payload)

    def test_execute_job_handles_keyframes_and_ocr(self):
        payload = {
            "schema_version": "v1",
            "dispatch_id": "dispatch-2",
            "candidate_id": "candidate-2",
            "candidate_key": "candidate-key-2",
            "canonical_url": "https://example.com/video",
            "callback_url": "https://callback.test/extraction",
            "callback_signature": "sig",
            "operations": ["keyframes", "ocr"],
            "metadata": {
                "youtube_context": {
                    "videoUrl": "https://cdn.example.com/video.mp4"
                }
            },
        }

        fake_frame = Path(tempfile.gettempdir()) / "frame-test.jpg"
        fake_frame.write_bytes(b"fake")
        artifacts = [{
            "kind": "image",
            "url": "data:image/jpeg;base64,ZmFrZQ==",
            "timestamp_ms": 0,
        }]

        with patch.object(run_extraction_job, "extract_keyframes", return_value=(artifacts, [fake_frame], None)):
            with patch.object(run_extraction_job, "extract_ocr_text", return_value=("Detected text", None)):
                result = run_extraction_job.execute_job(payload)

        self.assertEqual(result.callback_payload["status"], "success")
        self.assertEqual(result.callback_payload["operations_completed"], ["keyframes", "ocr"])
        self.assertEqual(result.callback_payload["evidence_updates"]["ocr_text"], "Detected text")
        self.assertEqual(result.callback_payload["evidence_updates"]["frame_artifacts"], artifacts)

    def test_execute_job_uses_same_runner_audio_fallback(self):
        payload = {
            "schema_version": "v1",
            "dispatch_id": "dispatch-3",
            "candidate_id": "candidate-3",
            "candidate_key": "candidate-key-3",
            "canonical_url": "https://www.youtube.com/watch?v=abc123",
            "callback_url": "https://callback.test/extraction",
            "callback_signature": "sig",
            "operations": ["subtitles", "audio_transcript"],
            "metadata": {
                "youtube_context": {
                    "videoUrl": "https://cdn.example.com/video.mp4"
                }
            },
        }

        with patch.object(run_extraction_job, "extract_subtitles", return_value=(None, "subtitles failed")):
            with patch.object(run_extraction_job, "extract_audio_transcript", return_value=("Recovered transcript", None)) as mock_audio:
                result = run_extraction_job.execute_job(payload)

        self.assertEqual(result.callback_payload["status"], "partial")
        self.assertEqual(result.callback_payload["operations_completed"], ["audio_transcript"])
        self.assertEqual(result.callback_payload["operations_failed"], ["subtitles"])
        self.assertEqual(result.callback_payload["evidence_updates"]["transcript"], "Recovered transcript")
        mock_audio.assert_called_once_with("https://www.youtube.com/watch?v=abc123", "https://cdn.example.com/video.mp4")


if __name__ == "__main__":
    unittest.main()
