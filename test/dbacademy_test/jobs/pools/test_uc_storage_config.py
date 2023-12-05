import unittest

from dbacademy.common import ValidationError
from dbacademy.jobs.pools.uc_storage_config_class import UcStorageConfig


if 'unittest.util' in __import__('sys').modules:
    # Show full diff in self.assertMultiLineEqual.
    # noinspection PyProtectedMember
    __import__('sys').modules['unittest.util']._MAX_LENGTH = 999999999


class TestUCStorageConfig(unittest.TestCase):

    def test_create(self):
        storage_config = UcStorageConfig(storage_root="def", storage_root_credential_id="ghi", region="jkl", meta_store_owner="instructors", aws_iam_role_arn="abc")
        self.assertIsNone(storage_config.meta_store_name)
        self.assertMultiLineEqual("def", storage_config.storage_root)
        self.assertMultiLineEqual("ghi", storage_config.storage_root_credential_id)
        self.assertMultiLineEqual("jkl", storage_config.region)

    # def test_create_meta_store_name(self):
    #     try:
    #         UcStorageConfig(meta_store_name=0, storage_root="def", storage_root_credential_id="ghi", region="jkl")
    #         self.fail("Expected ValidationError")
    #     except ValidationError as e:
    #         self.assertMultiLineEqual("x", e.message)
    #
    #     try:
    #         # noinspection PyTypeChecker
    #         UcStorageConfig(meta_store_name=None, storage_root="def", storage_root_credential_id="ghi", region="jkl")
    #         self.fail("Expected ValidationError")
    #     except ValidationError as e:
    #         self.assertMultiLineEqual("x", e.message)
    #
    #     try:
    #         UcStorageConfig(meta_store_name="", storage_root="def", storage_root_credential_id="ghi", region="jkl")
    #         self.fail("Expected ValidationError")
    #     except ValidationError as e:
    #         self.assertMultiLineEqual("x", e.message)

    def test_create_storage_root(self):
        try:
            # noinspection PyTypeChecker
            UcStorageConfig(storage_root=0, storage_root_credential_id="ghi", region="jkl", meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'storage_root' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            UcStorageConfig(storage_root=None, storage_root_credential_id="ghi", region="jkl", meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'storage_root' must be specified.", e.message)

        try:
            UcStorageConfig(storage_root="", storage_root_credential_id="ghi", region="jkl", meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'storage_root' must have a minimum length of 1, found 0.", e.message)

    def test_create_storage_root_credential_id(self):
        try:
            # noinspection PyTypeChecker
            UcStorageConfig(storage_root="def", storage_root_credential_id=0, region="jkl", meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'storage_root_credential_id' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            UcStorageConfig(storage_root="def", storage_root_credential_id=None, region="jkl", meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'storage_root_credential_id' must be specified.", e.message)

        try:
            UcStorageConfig(storage_root="def", storage_root_credential_id="", region="jkl", meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'storage_root_credential_id' must have a minimum length of 1, found 0.", e.message)

    def test_create_region(self):

        try:
            # noinspection PyTypeChecker
            UcStorageConfig(storage_root="def", storage_root_credential_id="ghi", region=0, meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'region' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            UcStorageConfig(storage_root="def", storage_root_credential_id="ghi", region=None, meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'region' must be specified.", e.message)

        try:
            UcStorageConfig(storage_root="def", storage_root_credential_id="ghi", region="", meta_store_owner="instructors", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'region' must have a minimum length of 1, found 0.", e.message)

    def test_create_owner(self):
        try:
            # noinspection PyTypeChecker
            UcStorageConfig(storage_root="def", storage_root_credential_id="ghi", region="jkl", meta_store_owner=0, aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'meta_store_owner' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            UcStorageConfig(storage_root="def", storage_root_credential_id="ghi", region="jkl", meta_store_owner=None, aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'meta_store_owner' must be specified.", e.message)

        try:
            UcStorageConfig(storage_root="def", storage_root_credential_id="ghi", region="jkl", meta_store_owner="", aws_iam_role_arn="abc")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'meta_store_owner' must have a minimum length of 1, found 0.", e.message)


if __name__ == '__main__':
    unittest.main()
