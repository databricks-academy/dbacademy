__all__ = ["SlackThread"]

from typing import List, Literal, Dict, Any, Optional, Union

COLOR_TYPE = Literal["good", "warning", "danger"]

class SlackThread(object):

    def __init__(self, channel, username, access_token):
        self.thread_ts = None
        self.initial_attachments = []
        self.last_response: Optional[Dict[str, Any]] = None
        self.channel = channel
        self.username = username
        self.access_token = access_token

        self.warnings = 0
        self.errors = 0
        self.exceptions = 0

    # def send_cc(self, users: Union[str, List[str]]):
    #     users_list = list()
    #
    #     if isinstance(users, str):
    #         users = list(users.split(","))
    #
    #     for i, user in enumerate(users):
    #         assert user.startswith("@"), f"""Expected item #{i+1} to start with "@", found "{user}"."""
    #         users_list.append(f"<{user}>")
    #         # users_list.append(user)
    #
    #     message = "cc " + (", ".join(users_list)) + " some other text"
    #
    #     json_payload = {
    #         "channel": self.channel,
    #         "username": self.username,
    #         "reply_broadcast": False,
    #         "text": self.__encode(message),
    #     }
    #
    #     if self.thread_ts:
    #         json_payload["thread_ts"] = self.thread_ts
    #
    #     self.__send(json_payload)

    def send_msg(self, message: str, reply_broadcast: bool = False, *, mentions: Union[str, List[str]] = None) -> Dict[str, Any]:
        encoded_message = self.__encode(message)
        json_payload = self._chat_payload(reply_broadcast, "good", encoded_message, attachments=None, mentions=mentions)
        self.__send(json_payload)

        return self.last_response

    def send_warning(self, message: str, reply_broadcast: bool = False, *, mentions: Union[str, List[str]] = None) -> Dict[str, Any]:
        encoded_message = self.__encode(message)
        json_payload = self._chat_payload(reply_broadcast, "warning", encoded_message, attachments=None, mentions=mentions)
        self.__send(json_payload)

        self.warnings += 1
        message, color = self._rebuild_first_message()
        self._update_first_msg(color, self.__encode(message))

        return self.last_response

    def send_error(self, message: str, reply_broadcast: bool = False, *, mentions: Union[str, List[str]] = None) -> Dict[str, Any]:
        encoded_message = self.__encode(message)
        json_payload = self._chat_payload(reply_broadcast, "danger", encoded_message, attachments=None, mentions=mentions)
        self.__send(json_payload)

        self.errors += 1
        message, color = self._rebuild_first_message()
        self._update_first_msg(color, self.__encode(message))

        return self.last_response

    def send_exception(self, message: str, reply_broadcast: bool = False, *, mentions: Union[str, List[str]] = None) -> Dict[str, Any]:
        import traceback

        message = self.__encode(message)

        error_msg = traceback.format_exc()

        if str(error_msg).strip() != "NoneType: None":
            message += "\n```{}```".format(error_msg)

        json_payload = self._chat_payload(reply_broadcast, "danger", message, attachments=None, mentions=mentions)

        self.__send(json_payload)

        self.exceptions += 1
        message, color = self._rebuild_first_message()
        self._update_first_msg(color, self.__encode(message))

        return self.last_response

    def _update_first_msg(self, color: COLOR_TYPE, message: str):
        encoded_message = self.__encode(message)
        json_payload = self.update_payload(color, encoded_message, self.initial_attachments)
        self.__send(json_payload, post=False)

    def _rebuild_first_message(self) -> (str, COLOR_TYPE):
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

    def __headers(self) -> Dict[str, Any]:
        assert self.access_token is not None, "Slack's OAuth Access Token must be specified"

        return {
            "Accept": "application/json; charset=utf-8",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {self.access_token}"
        }

    def __send(self, json_payload: Dict[str, Any], post: bool = True, attempts: int = 1) -> Dict[str, Any]:
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

        return self.last_response

    @classmethod
    def __encode(cls, text: str) -> str:
        import re

        """
        Encode: &, <, and > because slack uses these for control sequences.
        """
        text = re.sub("&", "&amp;", text)
        text = re.sub("<", "&lt;", text)
        text = re.sub(">", "&gt;", text)

        for match_obj in re.finditer("&lt;@.*&gt;", text):
            old_value = match_obj.group()
            new_value = f"<{old_value[4:-4]}>"
            text = text.replace(old_value, new_value)
            pass

        return text

    def update_payload(self, color: COLOR_TYPE, message: str, attachments: List[Dict[str, Any]]) -> Dict[str, Any]:
        assert len(attachments) > 0, f"""Expected at least one attachment."""

        attachments[0]["color"] = color
        attachments[0]["text"] = message

        ret_val = {
            "channel": self.channel,
            "username": self.username,
            "attachments": attachments,
            "ts": self.thread_ts
        }

        return ret_val

    def _chat_payload(self, reply_broadcast: bool, color: COLOR_TYPE, message: str, *, attachments: Optional[List[Dict[str, Any]]], mentions: Union[str, List[str]]) -> Dict[str, Any]:
        attachments = list() if attachments is None else attachments
        mentions = list() if mentions is None else mentions

        if isinstance(mentions, str):
            mentions = list(mentions.split(","))

        mentions_list = list()
        for i, mention in enumerate(mentions):
            mentions_list.append(f"<@{mention}>")

        if len(mentions_list) > 0:
            message += "\ncc " + (", ".join(mentions_list))

        attachment = {
            "color": color,
            "text": message,
        }
        if color is not None:
            attachment["mrkdwn_in"] = list("text")

        attachments.append(attachment)

        ret_val = {
            "channel": self.channel,
            "username": self.username,
            "reply_broadcast": reply_broadcast,
            "attachments": attachments
        }

        if self.thread_ts:
            ret_val["thread_ts"] = self.thread_ts

        return ret_val
