import unittest
from dbacademy.workspaces_3_0.storage_config_class import StorageConfig


class TestEventConfig(unittest.TestCase):

    def test_create_event_config(self):
        storage_config = StorageConfig(credentials_name="default", storage_configuration="us-west-2")
        self.assertEqual("default", storage_config.credentials_name)
        self.assertEqual("us-west-2", storage_config.storage_configuration)

    def test_create_event_config_credentials_name(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "credentials_name" must be a string value, found <class 'int'>.""", lambda: StorageConfig(credentials_name=0, storage_configuration="us-west-2"))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "credentials_name" must be a string value, found <class 'NoneType'>.""", lambda: StorageConfig(credentials_name=None, storage_configuration="us-west-2"))
        test_assertion_error(self, """The parameter "credentials_name" must be specified, found "".""", lambda: StorageConfig(credentials_name="", storage_configuration="us-west-2"))

    def test_create_event_config_storage_configuration(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "storage_configuration" must be a string value, found <class 'int'>.""", lambda: StorageConfig(credentials_name="default", storage_configuration=0))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "storage_configuration" must be a string value, found <class 'NoneType'>.""", lambda: StorageConfig(credentials_name="default", storage_configuration=None))
        test_assertion_error(self, """The parameter "storage_configuration" must be specified, found "".""", lambda: StorageConfig(credentials_name="default", storage_configuration=""))


if __name__ == '__main__':
    unittest.main()
