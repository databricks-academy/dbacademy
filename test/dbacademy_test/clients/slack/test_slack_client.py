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

    @property
    def test_name(self):
        import os
        return os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]

    def test_rebuild_first_message(self):
        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        thread = SlackThread(CHANNEL, "Slack Test", self.token)

        orig_message = f"*{self.test_name}*\nThis is a test"
        thread.send_msg(orig_message)

        thread.warnings = 3
        thread.errors = 0
        thread.exceptions = 0
        message, color = thread._rebuild_first_message()
        self.assertEqual(f"| 3 Warnings |\n{orig_message}", message)
        self.assertEqual(SlackThread.COLOR_WARNING, color)

        thread.warnings = 0
        thread.errors = 5
        thread.exceptions = 0
        message, color = thread._rebuild_first_message()
        self.assertEqual(f"| 5 Errors |\n{orig_message}", message)
        self.assertEqual(SlackThread.COLOR_DANGER, color)

        thread.warnings = 0
        thread.errors = 0
        thread.exceptions = 7
        message, color = thread._rebuild_first_message()
        self.assertEqual(f"| 7 Exceptions |\n{orig_message}", message)
        self.assertEqual(SlackThread.COLOR_DANGER, color)

        thread.warnings = 3
        thread.errors = 2
        thread.exceptions = 0
        message, color = thread._rebuild_first_message()
        self.assertEqual(f"| 2 Errors | 3 Warnings |\n{orig_message}", message)
        self.assertEqual(SlackThread.COLOR_DANGER, color)

        thread.warnings = 0
        thread.errors = 5
        thread.exceptions = 3
        message, color = thread._rebuild_first_message()
        self.assertEqual(f"| 3 Exceptions | 5 Errors |\n{orig_message}", message)
        self.assertEqual(SlackThread.COLOR_DANGER, color)

        thread.warnings = 1
        thread.errors = 0
        thread.exceptions = 7
        message, color = thread._rebuild_first_message()
        self.assertEqual(f"| 7 Exceptions | 1 Warnings |\n{orig_message}", message)
        self.assertEqual(SlackThread.COLOR_DANGER, color)

        thread.warnings = 1
        thread.errors = 2
        thread.exceptions = 7
        message, color = thread._rebuild_first_message()
        self.assertEqual(f"| 7 Exceptions | 2 Errors | 1 Warnings |\n{orig_message}", message)
        self.assertEqual(SlackThread.COLOR_DANGER, color)

    def test_split(self):
        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        text = "| 7 Exceptions | 2 Errors | 1 Warnings | Some random comment"
        parts = text.split("|")
        self.assertEqual("Some random comment", parts[-1].strip())

    def test_multi_line(self):
        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        message = f"*{self.test_name}*\nThis is a test with multiple lines.\nAnd this would be line #3"

        thread = SlackThread(CHANNEL, "Slack Test", self.token)
        self.assertIsNone(thread.thread_ts)
        self.assertIsNone(thread.last_response)

        thread.send_msg(message)
        self.assertIsNotNone(thread.thread_ts)
        self.assertIsNotNone(thread.last_response)

        thread.warnings = 1
        thread.errors = 2
        thread.exceptions = 3
        new_message, color = thread._rebuild_first_message()
        self.assertEqual(f"| 3 Exceptions | 2 Errors | 1 Warnings |\n{message}", new_message)
        self.assertEqual(SlackThread.COLOR_DANGER, color)

    def test_send_msg_with_mentions(self):
        from datetime import datetime

        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        thread = SlackThread(CHANNEL, "Slack Test", self.token)
        self.assertIsNone(thread.thread_ts)
        self.assertIsNone(thread.last_response)

        thread.send_msg(f"*{self.test_name}*\nThis is a test at {datetime.now()}", mentions=[SlackThread.MENTIONS.jacob_parr.handle,
                                                                         SlackThread.MENTIONS.jacob_parr.id,
                                                                         SlackThread.MENTIONS.lpt_alerts.handle,
                                                                         SlackThread.MENTIONS.lpt_alerts.id])
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg(f"This is a test to `{SlackThread.MENTIONS.jacob_parr.label}`'s handle", mentions=SlackThread.MENTIONS.jacob_parr.handle)
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg(f"This is a test to `{SlackThread.MENTIONS.jacob_parr.label}`'s id", mentions=SlackThread.MENTIONS.jacob_parr.id)
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg(f"This is a test to `{SlackThread.MENTIONS.lpt_alerts.label}`'s handle", mentions=SlackThread.MENTIONS.lpt_alerts.handle)
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg(f"This is a test to `{SlackThread.MENTIONS.lpt_alerts.label}`'s id", mentions=SlackThread.MENTIONS.lpt_alerts.id)
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

    def test_send_msg_with_default_mentions(self):
        from datetime import datetime

        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        thread = SlackThread(CHANNEL, "Slack Test", self.token, mentions=SlackThread.MENTIONS.jacob_parr.handle)
        self.assertIsNone(thread.thread_ts)
        self.assertIsNone(thread.last_response)

        thread.send_msg(f"*{self.test_name}*\nThis is the first message, sent at {datetime.now()}")
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg("This is the second message")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_warning("This is a warning")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_error("This is an error")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_exception("This is an exception")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

    def test_send_msg_for_thread(self):
        from datetime import datetime

        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        thread = SlackThread(CHANNEL, "Slack Test", self.token)
        self.assertIsNone(thread.thread_ts)
        self.assertIsNone(thread.last_response)

        thread.send_msg(f"*{self.test_name}*\nThis is a test at {datetime.now()}")
        first_ts = thread.thread_ts
        self.assertIsNotNone(first_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg("This is the second message\nSlackThread.MENTIONS.jacob_parr.handle", mentions=SlackThread.MENTIONS.jacob_parr.handle)
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        # noinspection PyTypeChecker
        thread._update_first_msg("#0000ff", f"*{self.test_name}*\nThis is the updated test at {datetime.now()}")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg("This is the third message\n[SlackThread.MENTIONS.jacob_parr.id]", mentions=[SlackThread.MENTIONS.jacob_parr.id])
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_warning("This is a warning\n[SlackThread.MENTIONS.jacob_parr.handle, SlackThread.MENTIONS.jacob_parr.id]", mentions=[SlackThread.MENTIONS.jacob_parr.handle, SlackThread.MENTIONS.jacob_parr.id])
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_error("This is an error\n[SlackThread.MENTIONS.jacob_parr, SlackThread.MENTIONS.lpt_alerts]", mentions=[SlackThread.MENTIONS.jacob_parr, SlackThread.MENTIONS.lpt_alerts])
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_exception("This is an exception\nSlackThread.MENTIONS.jacob_parr", mentions=SlackThread.MENTIONS.jacob_parr)
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

if __name__ == '__main__':
    unittest.main()
