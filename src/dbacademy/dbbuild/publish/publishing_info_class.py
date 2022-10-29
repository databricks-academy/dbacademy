from typing import Dict, List
from dbacademy import common


class PublishedDocs:
    def __init__(self, language: str, published_folder: str, publishing_script: str, links: List[str]):
        self.__language = language
        self.__published_folder = published_folder
        self.__publishing_script = publishing_script
        self.__links = links

    @property
    def language(self) -> str:
        return self.__language

    @property
    def published_folder(self) -> str:
        return self.__published_folder

    @property
    def publishing_script(self) -> str:
        return self.__publishing_script

    @property
    def links(self) -> List[str]:
        return self.__links


class SlackChannel:
    def __init__(self, name: str, link: str):
        self.__name = common.validate_type(name, "name", str)
        self.__link = common.validate_type(link, "link", str)

    @property
    def name(self):
        return self.__name

    @property
    def link(self):
        return self.__link


class Announcements:
    def __init__(self, email_addresses: List[str], slack_channels: List[SlackChannel]):
        self.__email_addresses = email_addresses
        self.__slack_channels = slack_channels

        common.validate_type(email_addresses, "email_addresses", List)
        common.validate_enumeration_type(email_addresses, "email_addresses", str)

        common.validate_type(slack_channels, "slack_channels", List)
        common.validate_enumeration_type(slack_channels, "slack_channels", SlackChannel)

    @property
    def email_addresses(self) -> List[str]:
        return self.__email_addresses

    @property
    def slack_channels(self) -> List[SlackChannel]:
        return self.__slack_channels


class PublishingInfo:

    def __init__(self, publishing_info: dict):
        self.__releases: Dict[str, str] = dict()
        self.__docs: Dict[str, PublishedDocs] = dict()

        # Load the list of releases repos
        release_info = publishing_info.get("release_repos")
        for language in release_info:
            self.__releases[language] = release_info[language]

        slides_info = publishing_info.get("slides")
        for language in slides_info:
            info = slides_info.get(language)
            published_folder = info.get("published_folder")
            publishing_script = info.get("publishing_script")

            slide_decks = info.get("slide_decks")
            self.__docs[language] = PublishedDocs(language, published_folder, publishing_script, slide_decks)

        announcements_info = publishing_info.get("announcements")
        channels = [SlackChannel(c.get("name"), c.get("link")) for c in announcements_info.get("slack_channels")]

        self.__announcements = Announcements(email_addresses=announcements_info.get("email_addresses"),
                                             slack_channels=channels)

    @property
    def releases(self) -> Dict[str, str]:
        return self.__releases

    @property
    def docs(self) -> Dict[str, PublishedDocs]:
        return self.__docs

    @property
    def announcements(self) -> Announcements:
        return self.__announcements
