__all__ = ["TestType", "TESTS_TYPES"]

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


TESTS_TYPES: TestConstants = TestConstants()

# noinspection PyTypeHints
TestType = Literal[TESTS_TYPES.INTERACTIVE, TESTS_TYPES.STOCK, TESTS_TYPES.PHOTON, TESTS_TYPES.ML]
