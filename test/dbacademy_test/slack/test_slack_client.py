import unittest
from dbacademy.slack.slack_thread import SlackThread


class MyTestCase(unittest.TestCase):

    @property
    def token(self):
        import os
        return os.getenv("SLACK_OAUTH_ACCESS_TOKEN", None)

    def test_rebuild_first_message(self):
        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        thread = SlackThread("training-engine-test", "Slack Test", self.token)
        thread.send_msg("This is a test")

        thread.warnings = 3
        thread.errors = 0
        thread.exceptions = 0
        message, color = thread.rebuild_first_message()
        self.assertEqual("| 3 Warnings |\nThis is a test", message)
        self.assertEqual("warning", color)

        thread.warnings = 0
        thread.errors = 5
        thread.exceptions = 0
        message, color = thread.rebuild_first_message()
        self.assertEqual("| 5 Errors |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 0
        thread.errors = 0
        thread.exceptions = 7
        message, color = thread.rebuild_first_message()
        self.assertEqual("| 7 Exceptions |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 3
        thread.errors = 2
        thread.exceptions = 0
        message, color = thread.rebuild_first_message()
        self.assertEqual("| 2 Errors | 3 Warnings |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 0
        thread.errors = 5
        thread.exceptions = 3
        message, color = thread.rebuild_first_message()
        self.assertEqual("| 3 Exceptions | 5 Errors |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 1
        thread.errors = 0
        thread.exceptions = 7
        message, color = thread.rebuild_first_message()
        self.assertEqual("| 7 Exceptions | 1 Warnings |\nThis is a test", message)
        self.assertEqual("danger", color)

        thread.warnings = 1
        thread.errors = 2
        thread.exceptions = 7
        message, color = thread.rebuild_first_message()
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

        thread = SlackThread("training-engine-test", "Slack Test", self.token)
        self.assertIsNone(thread.thread_ts)
        self.assertIsNone(thread.last_response)

        thread.send_msg(message)
        self.assertIsNotNone(thread.thread_ts)
        self.assertIsNotNone(thread.last_response)

        thread.warnings = 1
        thread.errors = 2
        thread.exceptions = 3
        message, color = thread.rebuild_first_message()
        self.assertEqual("| 3 Exceptions | 2 Errors | 1 Warnings |\nThis is a test with multiple lines.\nAnd this would be line #2", message)
        self.assertEqual("danger", color)

    def test_send_msg(self):
        if self.token is None:
            self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")

        from datetime import datetime

        current_time = datetime.now()
        first_message = f"This is a test at {current_time}"

        thread = SlackThread("training-engine-test", "Slack Test", self.token)
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

        thread.update_first_msg("#0000ff", f"This is the updated test at {current_time}")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_msg("This is the fourth message")
        self.assertEqual(first_ts, thread.thread_ts)
        self.assertIsNotNone(thread.last_response)
        self.assertTrue(thread.last_response["ok"])

        thread.send_warning("This is a warning")
        thread.send_error("This is an error")

        thread.send_exception("This is an exception")
        thread.send_exception("This is an exception")

        thread.send_error("This is an error")

        thread.send_warning("This is a warning")
        thread.send_warning("This is a warning")

    # def test_mention_message(self):
    # if self.token is None:
    #     self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")
    #
    #     who = "<@U5V5F358T>"
    #
    #     thread = SlackThread(f"training-engine-test", "Mentions Test", self.token)
    #     thread.send_msg(f"This is a message for {who}")
    #
    #     thread = SlackThread(f"training-engine-test", "Mentions Test", self.token)
    #     thread.send_warning(f"This is a warning for {who}")
    #
    #     thread = SlackThread(f"training-engine-test", "Mentions Test", self.token)
    #     thread.send_error(f"This is a error for {who}")
    #
    #     try:
    #         1 / 0
    #     except:
    #         thread = SlackThread(f"training-engine-test", "Mentions Test", self.token)
    #         thread.send_exception(f"This is an exception for {who}")
    #
    # def test_mention_thread(self):
    #     if self.token is None:
    #         self.skipTest("SLACK_OAUTH_ACCESS_TOKEN is not set")
    #
    #     who = "@U5V5F358T"
    #
    #     thread = SlackThread(f"training-engine-test", "Mentions Test", self.token)
    #     thread.send_msg(f"This is a message thread")
    #     thread.send_msg(f"Hey {who}")
    #
    #     thread = SlackThread(f"training-engine-test", "Mentions Test", self.token)
    #     thread.send_warning(f"This is a warning thread")
    #     thread.send_msg(f"Hey {who}")
    #
    #     thread = SlackThread(f"training-engine-test", "Mentions Test", self.token)
    #     thread.send_error(f"This is a error thread")
    #     thread.send_msg(f"Hey {who}")
    #
    #     try:
    #         1 / 0
    #     except:
    #         thread = SlackThread(f"training-engine-test", "Mentions Test", self.token)
    #         thread.send_exception(f"This is an exception thread")
    #     thread.send_msg(f"Hey {who}")


if __name__ == '__main__':
    unittest.main()
