__all__ = ["UcStorageConfig"]


class UcStorageConfig:
    def __init__(self, *, storage_root: str, storage_root_credential_id: str, region: str, owner: str):

        assert type(storage_root) == str, f"""The parameter "storage_root" must be a string value, found {type(storage_root)}."""
        assert len(storage_root) > 0, f"""The parameter "storage_root" must be specified, found "{storage_root}"."""

        assert type(storage_root_credential_id) == str, f"""The parameter "storage_root_credential_id" must be a string value, found {type(storage_root_credential_id)}."""
        assert len(storage_root_credential_id) > 0, f"""The parameter "storage_root_credential_id" must be specified, found "{storage_root_credential_id}"."""

        assert type(region) == str, f"""The parameter "region" must be a string value, found {type(region)}."""
        assert len(region) > 0, f"""The parameter "region" must be specified, found "{region}"."""

        assert type(owner) == str, f"""The parameter "owner" must be a string value, found {type(owner)}."""
        assert len(owner) > 0, f"""The parameter "owner" must be specified, found "{owner}"."""

        self.__meta_store_name = None
        self.__storage_root = storage_root
        self.__storage_root_credential_id = storage_root_credential_id
        self.__region = region
        self.__owner = owner

    @property
    def meta_store_name(self):
        return self.__meta_store_name

    @property
    def storage_root(self):
        return self.__storage_root

    @property
    def storage_root_credential_id(self):
        return self.__storage_root_credential_id

    @property
    def region(self):
        return self.__region

    @property
    def owner(self):
        return self.__owner
