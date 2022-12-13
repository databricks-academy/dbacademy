class SlackThread(object):

    def __init__(self, channel, username, access_token):
        self.thread_ts = None
        self.initial_attachments = []
        self.last_response = None
        self.channel = channel
        self.username = username
        self.access_token = access_token

        self.warnings = 0
        self.errors = 0
        self.exceptions = 0

    def update_first_msg(self, color, message):
        encoded = self.__encode(message)
        json_payload = self.update_payload(color, encoded, self.initial_attachments)
        self.__send(json_payload, post=False)

    def send_msg(self, message, reply_broadcast=False):
        encoded = self.__encode(message)
        json_payload = self.chat_payload(reply_broadcast, "good", encoded, [])
        self.__send(json_payload)

    def send_warning(self, message, reply_broadcast=False):
        encoded = self.__encode(message)
        json_payload = self.chat_payload(reply_broadcast, "warning", encoded, [])
        self.__send(json_payload)

        self.warnings += 1
        message, color = self.rebuild_first_message()
        self.update_first_msg(color, self.__encode(message))

    def send_error(self, message, reply_broadcast=False):
        encoded = self.__encode(message)
        json_payload = self.chat_payload(reply_broadcast, "danger", encoded, [])
        self.__send(json_payload)

        self.errors += 1
        message, color = self.rebuild_first_message()
        self.update_first_msg(color, self.__encode(message))

    def send_exception(self, message, reply_broadcast=False):
        import traceback

        message = self.__encode(message)

        error_msg = traceback.format_exc()

        if str(error_msg).strip() != "NoneType: None":
            message += "\n```{}```".format(error_msg)

        json_payload = self.chat_payload(reply_broadcast, "danger", message, [])

        self.__send(json_payload)

        self.exceptions += 1
        message, color = self.rebuild_first_message()
        self.update_first_msg(color, self.__encode(message))

    def rebuild_first_message(self):
        label = ""

        if self.exceptions > 0:
            label += f" {self.exceptions} Exceptions |"

        if self.errors > 0:
            label += f" {self.errors} Errors |"

        if self.warnings > 0:
            label += f" {self.warnings} Warnings |"

        label = label.strip()
        parts = self.initial_attachments[0]["text"].split("|\n")
        text = parts[-1].strip()
        message = f"| {label}\n{text}"

        color = "danger" if self.errors > 0 or self.exceptions > 0 else "warning"
        return message, color

    def __headers(self):
        assert self.access_token is not None, "Slack's OAuth Access Token must be specified"

        return {
            "Accept": "application/json; charset=utf-8",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {self.access_token}"
        }

    def __send(self, json_payload, post=True, attempts=1):
        import time
        import requests

        if attempts > 120:
            print("Failed to send Slack message after 120 attempts")
        elif attempts > 1:
            time.sleep(1)

        url = "https://slack.com/api/chat.postMessage" if post else "https://slack.com/api/chat.update"

        response = requests.post(
            url,
            headers=self.__headers(),
            json=json_payload)

        if response.status_code == 429:
            return self.__send(json_payload, post, attempts+1)
        elif response.status_code != 200:
            raise Exception("Unexpected response ({}):\n{}".format(response.status_code, response.text))

        # Slack reports 200 even when it actually isn't...
        # We have to go one step further and check the "ok" flag.
        self.last_response = response.json()
        if self.last_response["ok"] is not True:
            msg = self.last_response["error"] if "error" in self.last_response else "Unknown error"
            raise Exception("Unexpected response ({}):\n{}".format(response.status_code, msg))

        self.channel = self.last_response["channel"]

        if self.thread_ts is None:
            self.thread_ts = response.json()["ts"]
            self.initial_attachments = json_payload["attachments"]

    @staticmethod
    def __encode(text):
        import re

        """
        Encode: &, <, and > because slack uses these for control sequences.
        """
        text = re.sub("&", "&amp;", text)
        text = re.sub("<", "&lt;", text)
        text = re.sub(">", "&gt;", text)
        return text

    def update_payload(self, color, message, attachments):

        attachments[0]["color"] = color
        attachments[0]["text"] = message

        ret_val = {
            "channel": self.channel,
            "username": self.username,
            "attachments": attachments,
            "ts": self.thread_ts
        }

        return ret_val

    def chat_payload(self, reply_broadcast, color, message, attachments):

        attachments.append({
            "color": color,
            "text": message,
            "mrkdwn_in": ["text"],
        })

        ret_val = {
            "channel": self.channel,
            "username": self.username,
            "reply_broadcast": reply_broadcast,
            "attachments": attachments
        }

        if self.thread_ts:
            ret_val["thread_ts"] = self.thread_ts

        return ret_val
