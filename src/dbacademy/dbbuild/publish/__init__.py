__all__ = ["PublishingMode", "PUBLISH_TYPE"]

from typing import Literal, List


# noinspection PyPep8Naming
class PublishConstants:

    @property
    def MANUAL(self) -> str:
        return "interactive"

    @property
    def AUTOMATIC(self) -> str:
        return "automatic"

    @property
    def MODES(self) -> List[str]:
        return [self.MANUAL, self.AUTOMATIC]


PUBLISH_TYPE: PublishConstants = PublishConstants()

# noinspection PyTypeHints
PublishingMode = Literal[PUBLISH_TYPE.MANUAL, PUBLISH_TYPE.AUTOMATIC]
