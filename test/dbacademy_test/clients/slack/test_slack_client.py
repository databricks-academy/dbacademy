__all__ = ["SlackClientTests"]

import unittest
from dbacademy.clients.slack import SlackThread

# See #lpt-unit-tests
CHANNEL = "C060MJPUT0V"  # https://databricks.slack.com/archives/C060MJPUT0V


class SlackClientTests(unittest.TestCase):

    @property
    def token(self):
        import os
        return os.getenv("SLACK_OAUTH_ACCESS_TOKEN", None)

    def test_rebuild_first_message(self):
        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        thread = SlackThread(CHANNEL, "Slack Test", self.token)
        thread.send_msg("This is a test")

        thread.warnings = 3
        thread.errors = 0
        thread.exceptions = 0
        message, color = thread._rebuild_first_message()
        self.assertEqual("| 3 Warnings |\nThis is a test", message)
        self.assertEqual("warning", color)

        thread.warnings = 0
        thread.errors = 5
        thread.exceptions = 0
        message, color = thread._rebuild_first_message()
        self.assertEqual("| 5 Errors |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 0
        thread.errors = 0
        thread.exceptions = 7
        message, color = thread._rebuild_first_message()
        self.assertEqual("| 7 Exceptions |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 3
        thread.errors = 2
        thread.exceptions = 0
        message, color = thread._rebuild_first_message()
        self.assertEqual("| 2 Errors | 3 Warnings |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 0
        thread.errors = 5
        thread.exceptions = 3
        message, color = thread._rebuild_first_message()
        self.assertEqual("| 3 Exceptions | 5 Errors |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 1
        thread.errors = 0
        thread.exceptions = 7
        message, color = thread._rebuild_first_message()
        self.assertEqual("| 7 Exceptions | 1 Warnings |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 1
        thread.errors = 2
        thread.exceptions = 7
        message, color = thread._rebuild_first_message()
        self.assertEqual("| 7 Exceptions | 2 Errors | 1 Warnings |\nThis is a test", message)
        self.assertEqual("danger", color)

    def test_split(self):
        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        text = "| 7 Exceptions | 2 Errors | 1 Warnings | Some random comment"
        parts = text.split("|")
        self.assertEqual("Some random comment", parts[-1].strip())

    def test_multi_line(self):
        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        message = "This is a test with multiple lines.\nAnd this would be line #2"

        thread = SlackThread(CHANNEL, "Slack Test", self.token)
        self.assertIsNone(thread.thread_ts)
        self.assertIsNone(thread.last_response)

        thread.send_msg(message)
        self.assertIsNotNone(thread.thread_ts)
        self.assertIsNotNone(thread.last_response)

        thread.warnings = 1
        thread.errors = 2
        thread.exceptions = 3
        message, color = thread._rebuild_first_message()
        self.assertEqual("| 3 Exceptions | 2 Errors | 1 Warnings |\nThis is a test with multiple lines.\nAnd this would be line #2", message)
        self.assertEqual("danger", color)

    def test_send_msg_with_mentions(self):
        from datetime import datetime

        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        thread = SlackThread(CHANNEL, "Slack Test", self.token)
        self.assertIsNone(thread.thread_ts)
        self.assertIsNone(thread.last_response)

        thread.send_msg(f"This is a test at {datetime.now()}", mentions=["jacob.parr", "U5V5F358T"])
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg("This is a test to jacob.parr", mentions="jacob.parr")
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg("This is a test to U5V5F358T", mentions="U5V5F358T")
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

    def test_send_msg_for_thread(self):
        from datetime import datetime

        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        current_time = datetime.now()
        first_message = f"This is a test at {current_time}"

        thread = SlackThread(CHANNEL, "Slack Test", self.token)
        self.assertIsNone(thread.thread_ts)
        self.assertIsNone(thread.last_response)

        thread.send_msg(first_message)
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg("This is the second message")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        # noinspection PyTypeChecker
        thread._update_first_msg("#0000ff", f"This is the updated test at {current_time}")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg("This is the third message", mentions=["jacob.parr", "U5V5F358T"])
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_warning("This is a warning", mentions=["jacob.parr", "U5V5F358T"])
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_error("This is an error", mentions=["jacob.parr", "U5V5F358T"])
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_exception("This is an exception", mentions=["jacob.parr", "U5V5F358T"])
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

if __name__ == '__main__':
    unittest.main()
