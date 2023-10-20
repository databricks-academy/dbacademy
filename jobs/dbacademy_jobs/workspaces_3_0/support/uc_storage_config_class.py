from typing import Optional

__all__ = ["UcStorageConfig"]


class UcStorageConfig:
    def __init__(self, *,
                 storage_root: str,
                 storage_root_credential_id: str,
                 region: str,
                 meta_store_owner: str,
                 aws_iam_role_arn: Optional[str],
                 msa_access_connector_id: Optional[str]):
        from dbacademy.common import validate

        self.__meta_store_name = None
        self.__storage_root = validate.str_value(min_length=1, storage_root=storage_root)
        self.__storage_root_credential_id = validate.str_value(min_length=1, storage_root_credential_id=storage_root_credential_id)
        self.__region = validate.str_value(min_length=1, region=region)
        self.__meta_store_owner = validate.str_value(min_length=1, meta_store_owner=meta_store_owner)
        self.__aws_iam_role_arn = validate.str_value(aws_iam_role_arn=aws_iam_role_arn)
        self.__msa_access_connector_id = validate.str_value(msa_access_connector_id=msa_access_connector_id)

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
    def meta_store_owner(self):
        return self.__meta_store_owner

    @property
    def aws_iam_role_arn(self) -> str:
        return self.__aws_iam_role_arn

    @property
    def msa_access_connector_id(self) -> str:
        return self.__msa_access_connector_id
