import asyncio
import unittest
from unittest.mock import patch
from google.api_core.exceptions import NotFound

from services.listener.src import processing

class TestProcessNewFiling(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Save original global state
        self.orig_publisher = processing.publisher
        self.orig_topic_path = processing.topic_path

    async def asyncTearDown(self):
        # Restore original global state
        processing.publisher = self.orig_publisher
        processing.topic_path = self.orig_topic_path

    async def test_no_publisher_client(self):
        """If publisher is None, should log an error and return without exception."""
        processing.publisher = None
        with self.assertLogs(level="ERROR") as cm:
            await processing.process_new_filing(
                "TICK", "ACC-1", "10-K", "http://example.com", "2024-01-01"
            )
        log_output = "\n".join(cm.output)
        self.assertIn(
            "Pub/Sub publisher client is not initialized. Cannot publish message.",
            log_output
        )

    @patch("services.listener.src.processing.publisher.publish")
    async def test_successful_publish(self, mock_publish):
        """Successful path: publish → wrap_future → INFO logs."""
        # publisher.publish returns a future with a known result
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        fut.set_result("message-id-123")
        mock_publish.return_value = fut

        # Override only topic_path so logs are predictable
        processing.topic_path = "projects/p/dummy-topic"

        # Patch wrap_future to immediately return the future's result
        async def fake_wrap(future):
            return future.result()

        with patch(
            "services.listener.src.processing.asyncio.wrap_future",
            new=fake_wrap
        ):
            with self.assertLogs(level="INFO") as cm:
                await processing.process_new_filing(
                    "TICK", "ACC-2", "10-Q", "url", "2024-02-02"
                )

        logs = "\n".join(cm.output)
        self.assertIn(
            "Publishing message for TICK (ACC-2) to projects/p/dummy-topic",
            logs
        )
        self.assertIn(
            "Successfully published message for ACC-2. Message ID: message-id-123",
            logs
        )

    @patch(
        "services.listener.src.processing.publisher.publish",
        side_effect=NotFound("topic-missing")
    )
    async def test_publish_not_found(self, mock_publish):
        """If the Pub/Sub topic doesn't exist, should catch NotFound and log an error."""
        processing.topic_path = "projects/p/dummy-topic"
        with self.assertLogs(level="ERROR") as cm:
            await processing.process_new_filing(
                "T", "ACC-3", "8-K", "url", "2024-03-03"
            )
        log_output = "\n".join(cm.output)
        self.assertIn(
            "Pub/Sub topic projects/p/dummy-topic not found during publish operation.",
            log_output
        )

    @patch(
        "services.listener.src.processing.publisher.publish",
        side_effect=ValueError("oops")
    )
    async def test_publish_generic_exception(self, mock_publish):
        """On any other exception, should log with logging.exception."""
        processing.topic_path = "projects/p/dummy-topic"
        with self.assertLogs(level="ERROR") as cm:
            await processing.process_new_filing(
                "T", "ACC-4", "10-K", "url", "2024-04-04"
            )
        log_output = "\n".join(cm.output)
        self.assertIn(
            "Failed to publish message for ACC-4: oops",
            log_output
        )

if __name__ == "__main__":
    unittest.main()
