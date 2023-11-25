__all__ = ["Advertiser"]

from typing import Optional
from dbacademy.dbbuild.change_log import ChangeLog
from dbacademy.dbbuild.publish.publishing_info import PublishingInfo


class Advertiser:

    def __init__(self, *,
                 name: str,
                 version: str,
                 change_log: ChangeLog,
                 publishing_info: PublishingInfo,
                 source_repo: str,
                 common_language: Optional[str]):

        self.__name = name
        self.__version = version
        self.__change_log = change_log
        self.__publishing_info = publishing_info
        self.__source_repo = source_repo
        self.__common_language = common_language

        self.__create_message()

        self.__subject = f"Published {self.__name}, v{self.__version}"

        self.__create_html()

    @property
    def name(self) -> str:
        return self.__name

    @property
    def version(self) -> str:
        return self.__version

    @property
    def change_log(self) -> ChangeLog:
        return self.__change_log

    @property
    def publishing_info(self) -> PublishingInfo:
        return self.__publishing_info

    @property
    def source_repo(self) -> str:
        return self.__source_repo

    @property
    def common_language(self) -> Optional[str]:
        return self.__common_language

    @property
    def html(self) -> str:
        return self.__html

    def __create_message(self) -> None:
        self.__message = f"""{self.__change_log}
        
Release notes, course-specific requirements, issue-tracking, and test results for this course can be found in the course's GitHub repository at https://github.com/databricks-academy/{self.__source_repo.split("/")[-1]}

Please contact me (via Slack), or anyone on the curriculum team should you have any questions."""

    def __create_html(self) -> None:
        import urllib.parse

        email_subject = urllib.parse.quote(self.__subject, safe="")
        email_body = urllib.parse.quote(self.__message, safe="")

        content = """<div style="margin-bottom:1em">"""

        for address in self.__publishing_info.announcements.email_addresses:
            url = f"mailto:{address}?subject={email_subject}&body={email_body}"
            content += f"""<li><a href="{url}" target="_blank">{address}</a></li>"""

        for channel in self.__publishing_info.announcements.slack_channels:
            content += f"""<li><a href="{channel.url}" target="_blank">{channel.name}</a></li>"""

        content += "</div>"

        rows = len(self.__message.split("\n")) + 2
        self.__html = f"""
        <body style="font-size:16px">
            {content}
            <div><input style="width:100%; padding:1em" type="text" value="{self.__subject}"></div>
            <textarea style="width:100%; padding:1em" rows={rows}>{self.__message}</textarea>
        </body>"""
