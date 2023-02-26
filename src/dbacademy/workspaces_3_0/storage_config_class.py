__all__ = ["StorageConfig"]


class StorageConfig:
    def __init__(self, *, credentials_name: str, storage_configuration: str):

        assert type(credentials_name) == str, f"""The parameter "credentials_name" must be a string value, found {type(credentials_name)}."""
        assert len(credentials_name) > 0, f"""The parameter "credentials_name" must be specified, found "{credentials_name}"."""

        assert type(storage_configuration) == str, f"""The parameter "storage_configuration" must be a string value, found {type(storage_configuration)}."""
        assert len(storage_configuration) > 0, f"""The parameter "storage_configuration" must be specified, found "{storage_configuration}"."""

        self.__credentials_name = credentials_name
        self.__storage_configuration = storage_configuration

    @property
    def credentials_name(self):
        return self.__credentials_name

    @property
    def storage_configuration(self):
        return self.__storage_configuration
