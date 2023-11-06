__all__ = ["SlackThread", "from_args", "GOOD", "WARNING", "DANGER"]

from typing import List, Dict, Any, Optional, Union
from dbacademy.common import validate


class Level:
    def __init__(self, color: str):
        self.color = color


GOOD = Level("good")
WARNING = Level("warning")
DANGER = Level("danger")


class Mention:

    def __init__(self, _label: str, _handle: str, _id: Optional[str]):
        self.label = _label
        self.handle = _handle
        self.id = _id


class Mentions:

    def __init__(self):
        self.jacob_parr = Mention("Jacob Parr", "@jacob.parr", "@U5V5F358T")
        self.mylene_biddle = Mention("Mylene Biddle", "@mylene.biddle", "@U05J73W61EJ")
        self.lpt_alerts = Mention("LPT Alerts", "!subteam^SQDKRFZF0", None)


MENTIONS = Mentions()


class SlackThread(object):

    def __init__(self, channel: str, username: str, access_token: str, mentions: Optional[Union[str, Mention, List[str]]]):
        self.thread_ts = None
        self.initial_attachments = list()
        self.last_response: Optional[Dict[str, Any]] = None
        self.channel = channel
        self.username = username
        self.access_token = access_token

        mentions = list() if mentions is None else mentions
        mentions = mentions.handle if isinstance(mentions, Mention) else mentions
        mentions = list(mentions.split(",")) if isinstance(mentions, str) else mentions
        self.__mentions = mentions

        self.warnings = 0
        self.errors = 0
        self.exceptions = 0

    def send_msg(self, message: str, reply_broadcast: bool = False, *, mentions: Union[str, Mention, List[str]] = None) -> Dict[str, Any]:
        encoded_message = self.__encode(message)
        json_payload = self._chat_payload(reply_broadcast, GOOD, encoded_message, attachments=None, mentions=mentions)
        self.__send(json_payload)

        return self.last_response

    def send_warning(self, message: str, reply_broadcast: bool = False, *, mentions: Union[str, Mention, List[str]] = None) -> Dict[str, Any]:
        encoded_message = self.__encode(message)
        json_payload = self._chat_payload(reply_broadcast, WARNING, encoded_message, attachments=None, mentions=mentions)
        self.__send(json_payload)

        self.warnings += 1
        message, color = self._rebuild_first_message()
        self._update_first_msg(color, self.__encode(message))

        return self.last_response

    def send_error(self, message: str, reply_broadcast: bool = False, *, mentions: Union[str, Mention, List[str]] = None) -> Dict[str, Any]:
        encoded_message = self.__encode(message)
        json_payload = self._chat_payload(reply_broadcast, DANGER, encoded_message, attachments=None, mentions=mentions)
        self.__send(json_payload)

        self.errors += 1
        message, color = self._rebuild_first_message()
        self._update_first_msg(color, self.__encode(message))

        return self.last_response

    def send_exception(self, message: str, reply_broadcast: bool = False, *, mentions: Union[str, Mention, List[str]] = None) -> Dict[str, Any]:
        import traceback

        message = self.__encode(message)

        error_msg = traceback.format_exc()

        if str(error_msg).strip() != "NoneType: None":
            message += "\n```{}```".format(error_msg)

        json_payload = self._chat_payload(reply_broadcast, DANGER, message, attachments=None, mentions=mentions)

        self.__send(json_payload)

        self.exceptions += 1
        message, color = self._rebuild_first_message()
        self._update_first_msg(color, self.__encode(message))

        return self.last_response

    def _update_first_msg(self, level: Level, message: str):
        encoded_message = self.__encode(message)
        json_payload = self._update_payload(level, encoded_message, self.initial_attachments)
        self.__send(json_payload, post=False)

    def _rebuild_first_message(self) -> (str, Level):
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

        color = DANGER if self.errors > 0 or self.exceptions > 0 else WARNING
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

    def _update_payload(self, level: Level, message: str, attachments: List[Dict[str, Any]]) -> Dict[str, Any]:
        validate.any_value(Level, level=level, required=True)
        validate.str_value(message=message, required=True)
        validate.list_of_type(attachments=attachments, element_type=Dict, required=True)

        assert len(attachments) > 0, f"""Expected at least one attachment."""

        attachments[0]["color"] = level.color
        attachments[0]["text"] = message

        ret_val = {
            "channel": self.channel,
            "username": self.username,
            "attachments": attachments,
            "ts": self.thread_ts
        }

        return ret_val

    def _chat_payload(self, reply_broadcast: bool, level: Level, message: str, *, attachments: Optional[List[Dict[str, Any]]], mentions: Union[str, Mention, List[str]]) -> Dict[str, Any]:
        attachments = list() if attachments is None else attachments

        mentions = list() if mentions is None else mentions
        mentions = mentions.handle if isinstance(mentions, Mention) else mentions
        mentions = list(mentions.split(",")) if isinstance(mentions, str) else mentions

        if level != GOOD:
            # Add thread mentions only if it's not good (e.g. warning or danger)
            mentions.extend(self.__mentions)

        mentions_list = list()
        for i, mention in enumerate(mentions):
            if isinstance(mention, Mention):
                mentions_list.append(f"<{mention.handle}>")
            else:
                mentions_list.append(f"<{mention}>")

        if len(mentions_list) > 0:
            message += "\ncc " + (", ".join(mentions_list))

        attachment = {
            "color": level.color,
            "text": message,
        }

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


def from_args(*,
              channel: str,
              username: str,
              access_token: str,
              mentions: Union[str, Mention, List[str]] = None):

    return SlackThread(channel=channel,
                       username=username,
                       access_token=access_token,
                       mentions=mentions)


def from_environment(*,
                     # scope: str = "SLACK",
                     channel: str,
                     username: str,
                     access_token: str = None,
                     mentions: Union[str, Mention, List[str]] = None):
    import os
    scope = "SLACK"

    return SlackThread(channel=channel,
                       username=username,
                       access_token=access_token or os.environ.get(f"{scope}_TOKEN") or os.environ.get("TOKEN"),
                       mentions=mentions)
