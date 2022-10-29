class Advertiser:
    from dbacademy.dbbuild.change_log_class import ChangeLog
    from dbacademy.dbbuild.publish.publishing_info_class import PublishingInfo

    def __init__(self, *, name: str, version: str, change_log: ChangeLog, publishing_info: PublishingInfo, source_repo: str, common_language):
        import urllib.parse

        self.__name = name
        self.__version = version
        self.__change_log = change_log
        self.__publishing_info = publishing_info
        self.__source_repo = source_repo
        self.__common_language = common_language

        self.__create_message()

        self.__subject = f"Published {self.__name}, v{self.__version}"
        self.__email_body = urllib.parse.quote(self.__message, safe="")

        self.__slack_message = f"@channel {self.__subject}\n\n{self.__message.strip()}"

        self.__create_html()

    @property
    def html(self):
        return self.__html

    def __create_message(self):
        self.__message = str(self.__change_log)
        self.__message += "\n"
        self.__message += f"""
Release notes, course-specific requirements, issue-tracking, and test results for this course can be found in the course's GitHub repository at https://github.com/databricks-academy/{self.__source_repo.split("/")[-1]}

Please contact me (via Slack), or anyone on the curriculum team should you have any questions."""

    def __create_html(self):
        content = """<div style="margin-bottom:1em>"""

        for address in self.__publishing_info.announcements.email_addresses:
            url = f"mailto:{address}?subject={self.__subject}&body={self.__email_body}"
            content += f"""<li><a href="{url}" target="_blank">{address}</a></li>"""

        for channel in self.__publishing_info.announcements.slack_channels:
            content += f"""<li><a href="{channel.url}" target="_blank">{channel.name}</a></li>"""

        content += "</div>"

        rows = len(self.__slack_message.split("\n")) + 1
        self.__html = f"""
        <body style="font-size:16px">
            {content}
            <textarea style="width:100%; padding:1em" rows={rows}>{self.__slack_message}</textarea>
        </body>"""
