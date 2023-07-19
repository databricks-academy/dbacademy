__all__ = ["ClientErrorHandler"]


class ClientErrorHandler:

    # noinspection PyMethodMayBeStatic
    def on_error(self, *messages: str):
        for message in messages:
            print(message)
