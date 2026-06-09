import http.client
import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer

from app.analyzer import get_analyzer
from app.server import Handler


class ServerTest(unittest.TestCase):
    def test_rejects_non_object_messages(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
            connection.request(
                "POST",
                "/analyze/chat-bucket",
                body=json.dumps({"messages": [None]}),
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            body = json.loads(response.read())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertEqual(response.status, 400)
        self.assertEqual(body["error"], "messages must contain only objects")

    def test_metrics_endpoint_returns_prometheus_text(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
            connection.request("GET", "/metrics")
            response = connection.getresponse()
            body = response.read().decode("utf-8")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertEqual(response.status, 200)
        self.assertIn("text/plain", response.getheader("Content-Type"))
        self.assertIn("# TYPE sentiment_analyzer_requests_total counter", body)
        self.assertIn("sentiment_analyzer_analyze_latency_ms_count", body)

    def test_health_reports_configured_lexicon_backend(self):
        previous_backend = os.environ.get("SENTIMENT_BACKEND")
        previous_model = os.environ.get("SENTIMENT_MODEL")
        os.environ["SENTIMENT_BACKEND"] = "lexicon"
        os.environ["SENTIMENT_MODEL"] = "local-lexicon"
        get_analyzer.cache_clear()
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
            connection.request("GET", "/health")
            response = connection.getresponse()
            body = json.loads(response.read())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
            if previous_backend is None:
                os.environ.pop("SENTIMENT_BACKEND", None)
            else:
                os.environ["SENTIMENT_BACKEND"] = previous_backend
            if previous_model is None:
                os.environ.pop("SENTIMENT_MODEL", None)
            else:
                os.environ["SENTIMENT_MODEL"] = previous_model
            get_analyzer.cache_clear()

        self.assertEqual(response.status, 200)
        self.assertEqual(body["backend"], "lexicon")
        self.assertEqual(body["model"], "local-lexicon")


if __name__ == "__main__":
    unittest.main()
