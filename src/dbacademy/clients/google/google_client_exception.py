__all__ = ["GoogleClientException"]


class GoogleClientException(Exception):
    def __init__(self, status_code: int, message: str):
        self.__message = message
        self.__status_code = status_code

    def __str__(self):
        if self.__status_code == 0:
            return self.message
        else:
            return f"{self.status_code}: {self.message}"

    @property
    def message(self) -> str:
        return self.__message

    @property
    def status_code(self) -> int:
        return self.__status_code
