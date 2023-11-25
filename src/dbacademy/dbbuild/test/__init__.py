__all__ = ["TestType", "TEST_TYPE"]

from typing import Literal, List


# noinspection PyPep8Naming
class TestConstants:

    @property
    def INTERACTIVE(self) -> str:
        return "interactive"

    @property
    def STOCK(self) -> str:
        return "stock"

    @property
    def PHOTON(self) -> str:
        return "photon"

    @property
    def ML(self) -> str:
        return "ml"

    @property
    def TYPES(self) -> List[str]:
        return [self.INTERACTIVE, self.STOCK, self.PHOTON, self.ML]


TEST_TYPE: TestConstants = TestConstants()

# noinspection PyTypeHints
TestType = Literal[TEST_TYPE.INTERACTIVE, TEST_TYPE.STOCK, TEST_TYPE.PHOTON, TEST_TYPE.ML]
