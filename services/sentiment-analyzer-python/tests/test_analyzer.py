import threading
import time
import unittest

from app.analyzer import TransformersSentimentAnalyzer


class FakeClassifier:
    def __init__(self):
        self.calls = []

    def __call__(self, texts, **kwargs):
        self.calls.append({"texts": list(texts), "kwargs": dict(kwargs)})
        outputs = []
        for text in texts:
            lowered = text.lower()
            if "bad" in lowered or "trash" in lowered:
                outputs.append({"label": "negative", "score": 0.95})
            elif "great" in lowered or "clutch" in lowered or lowered.startswith("w "):
                outputs.append({"label": "positive", "score": 0.90})
            else:
                outputs.append({"label": "neutral", "score": 0.80})
        return outputs


class AnalyzerTest(unittest.TestCase):
    def test_aggregates_bucket_sentiment(self):
        analyzer = TransformersSentimentAnalyzer(
            model_name="test-model",
            classifier=FakeClassifier(),
            batch_size=16,
            trace_limit=3,
        )
        result = analyzer.analyze_bucket(
            [
                {"text": "W stream great clutch"},
                {"text": "this is bad trash"},
                {"text": "just chatting"},
                {"text": "W stream great clutch"},
            ]
        )

        self.assertEqual(result["message_count"], 4)
        self.assertEqual(result["analyzed_count"], 4)
        self.assertEqual(result["analysis_message_limit"], 32)
        self.assertGreater(result["sentiment_score"], 0)
        self.assertGreater(result["positive"], result["negative"])
        self.assertEqual(result["model"], "test-model")
        self.assertEqual(len(result["message_scores"]), 3)
        self.assertEqual(result["message_scores"][0]["label"], "positive")

    def test_empty_bucket_returns_neutral(self):
        analyzer = TransformersSentimentAnalyzer(model_name="test-model", classifier=FakeClassifier())
        result = analyzer.analyze_bucket([{"text": ""}, {"text": "   "}])

        self.assertEqual(result["message_count"], 2)
        self.assertEqual(result["analyzed_count"], 0)
        self.assertEqual(result["neutral"], 1.0)
        self.assertEqual(result["model"], "test-model")
        self.assertEqual(result["message_scores"], [])

    def test_caps_large_buckets_before_model_inference(self):
        classifier = FakeClassifier()
        analyzer = TransformersSentimentAnalyzer(
            model_name="test-model",
            classifier=classifier,
            batch_size=16,
            max_messages=5,
            trace_limit=10,
        )

        result = analyzer.analyze_bucket([{"text": f"message {index}"} for index in range(20)])

        self.assertEqual(result["message_count"], 20)
        self.assertEqual(result["analyzed_count"], 5)
        self.assertEqual(result["analysis_message_limit"], 5)
        self.assertEqual(len(classifier.calls), 1)
        self.assertEqual(classifier.calls[0]["texts"], ["message 0", "message 5", "message 10", "message 14", "message 19"])
        self.assertEqual(classifier.calls[0]["kwargs"]["batch_size"], 16)

    def test_peak_window_messages_are_sampled_first(self):
        classifier = FakeClassifier()
        analyzer = TransformersSentimentAnalyzer(
            model_name="test-model",
            classifier=classifier,
            max_messages=5,
            trace_limit=10,
        )
        messages = [
            {"message_id": f"m{index}", "timestamp": f"2026-05-08T12:00:{index:02d}Z", "text": f"message {index}"}
            for index in range(10)
        ]

        analyzer.analyze_bucket(
            messages,
            peak_window_start="2026-05-08T12:00:04Z",
            peak_window_end="2026-05-08T12:00:07Z",
        )

        self.assertEqual(classifier.calls[0]["texts"][:3], ["message 4", "message 5", "message 6"])
        self.assertEqual(len(classifier.calls[0]["texts"]), 5)

    def test_truncates_long_text_before_model_inference(self):
        classifier = FakeClassifier()
        analyzer = TransformersSentimentAnalyzer(
            model_name="test-model",
            classifier=classifier,
            max_text_chars=8,
        )

        result = analyzer.analyze_bucket([{"text": "great clutch with a very long tail"}])

        self.assertEqual(classifier.calls[0]["texts"], ["great cl"])
        self.assertEqual(result["message_scores"][0]["text"], "great cl")

    def test_truncated_text_is_stripped_consistently(self):
        classifier = FakeClassifier()
        analyzer = TransformersSentimentAnalyzer(
            model_name="test-model",
            classifier=classifier,
            max_text_chars=10,
        )

        result = analyzer.analyze_bucket([{"text": "great     clutch"}])

        self.assertEqual(classifier.calls[0]["texts"], ["great"])
        self.assertEqual(result["message_scores"][0]["text"], "great")

    def test_concurrent_analyze_calls_do_not_overlap_classifier_inference(self):
        class BlockingClassifier:
            def __init__(self):
                self.lock = threading.Lock()
                self.calls = 0
                self.active_calls = 0
                self.max_active_calls = 0

            def __call__(self, texts, **kwargs):
                with self.lock:
                    self.calls += 1
                    self.active_calls += 1
                    self.max_active_calls = max(self.max_active_calls, self.active_calls)
                time.sleep(0.05)
                with self.lock:
                    self.active_calls -= 1
                return [{"label": "positive", "score": 0.9} for _ in texts]

        classifier = BlockingClassifier()
        analyzer = TransformersSentimentAnalyzer(model_name="test-model", classifier=classifier)
        start = threading.Barrier(3)
        errors = []

        def analyze_message(text):
            try:
                start.wait(timeout=2)
                analyzer.analyze_bucket([{"text": text}])
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=analyze_message, args=("great one",)),
            threading.Thread(target=analyze_message, args=("great two",)),
        ]
        for thread in threads:
            thread.start()
        start.wait(timeout=2)
        for thread in threads:
            thread.join(timeout=2)

        self.assertEqual(errors, [])
        self.assertEqual(classifier.calls, 2)
        self.assertEqual(classifier.max_active_calls, 1)


if __name__ == "__main__":
    unittest.main()
