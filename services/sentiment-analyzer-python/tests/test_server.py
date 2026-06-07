import http.client
import json
import threading
import unittest
from http.server import ThreadingHTTPServer

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


if __name__ == "__main__":
    unittest.main()
