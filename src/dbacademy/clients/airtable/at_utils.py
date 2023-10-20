__all__ = ["AirTableUtils"]

from requests import Response
from dbacademy.clients import ClientErrorHandler


class AirTableUtils:

    ATTEMPTS = range(1, 10 + 1)  # 1-10 inclusive

    def __init__(self, error_handler: ClientErrorHandler):
        self.error_handler = error_handler

    @classmethod
    def is_rate_limited(cls, response: Response, attempt: int) -> bool:
        import time
        import random

        rate_limited = response.text is not None and "RATE_LIMIT_REACHED" in response.text
        if rate_limited:
            time.sleep(attempt * random.random())

        return rate_limited

    def validate_response(self, response: Response, error_message: str, status_code: int) -> None:
        import json

        if response.status_code != 200:
            # Almost a "perfect" API except all successful calls return 200 <groan>
            message = f"""{error_message}: {status_code}"""
            self.error_handler.on_error(message, json.dumps(response.json(), indent=4))
            raise AssertionError(message)

    def raise_rate_limit_failure(self, what: str) -> None:
        error_message = f"""{what}; failed after {self.ATTEMPTS}."""
        self.error_handler.on_error(error_message)
        raise AssertionError(error_message)
