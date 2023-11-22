__all__ = ["SlackChannel", "Announcements", "Translation", "PublishingInfo"]

from typing import Dict, List, Any
from dbacademy.common import validate


class SlackChannel:
    def __init__(self, name: str, url: str):
        self.__url = validate(url=url).str()
        self.__name = validate(name=name).str()

    @property
    def name(self):
        return self.__name

    @property
    def url(self):
        return self.__url


class Announcements:
    def __init__(self, email_addresses: List[str], slack_channels: List[SlackChannel]):
        self.__email_addresses = validate(email_addresses=email_addresses).list(str, auto_create=True)
        self.__slack_channels = validate(slack_channels=slack_channels).list(element_type=SlackChannel, auto_create=True)

    @property
    def email_addresses(self) -> List[str]:
        return self.__email_addresses

    @property
    def slack_channels(self) -> List[SlackChannel]:
        return self.__slack_channels


class Translation:
    def __init__(self, language, data: Dict[str, Any]):
        self.__language = language
        self.__release_repo = validate(release_repo=data.get("release_repo")).str()
        self.__published_docs_folder = validate(published_docs_folder=data.get("published_docs_folder")).str()
        self.__document_links = validate(document_links=data.get("document_links")).list(str, auto_create=True)

    @property
    def language(self):
        return self.__language

    @property
    def release_repo(self) -> str:
        return self.__release_repo

    @property
    def published_docs_folder(self) -> str:
        return self.__published_docs_folder

    @property
    def document_links(self) -> List[str]:
        return self.__document_links


class PublishingInfo:

    def __init__(self, publishing_info: Dict[str, Any]):
        a = publishing_info.get("announcements")
        self.__announcements = Announcements(email_addresses=a.get("email_addresses"),
                                             slack_channels=[SlackChannel(name=c.get("name"), url=c.get("url")) for c in a.get("slack_channels")])

        self.__translations: Dict[str, Translation] = {}
        translations: Dict = publishing_info.get("translations")

        for language, data in translations.items():
            self.__translations[language] = Translation(language, data)

    @property
    def announcements(self) -> Announcements:
        return self.__announcements

    @property
    def translations(self) -> Dict[str, Translation]:
        return self.__translations
