import unittest, pytest
from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.event_config_class import EventConfig
from dbacademy.workspaces_3_0.storage_config_class import StorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig


class TestAccountConfig(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self):
        self.event_config = EventConfig(event_id=999, max_participants=300)
        self.storage_config = StorageConfig(credentials_name="default", storage_configuration="us-west-2")

        dbc_urls = ["https://labs.training.databricks.com/api/courses?course=example-course&version=vLATEST&artifact=lessons.dbc&token=abcd"]
        self.workspace_config = WorkspaceConfig(max_user_count=100, courses="example-course", datasets="example-course", default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", dbc_urls=dbc_urls)

    def test_from_env(self):
        account = AccountConfig.from_env(region="us-west-2", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config)
        self.assertIsNotNone(account)

        self.assertEqual("b6e87bd6-c65c-46d3-abde-6840bf706d39", account.account_id)
        self.assertEqual("class+curriculum@databricks.com", account.username)
        self.assertTrue(account.password.startswith("54"))
        self.assertTrue(account.password.endswith("V1"))

    def test_create_account_config(self):

        account = AccountConfig(region="us-west-2", account_id="asdf", username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config)

        self.assertEqual("us-west-2", account.region)
        self.assertEqual("asdf", account.account_id)
        self.assertEqual("mickey.mouse@disney.com", account.username)
        self.assertEqual("whatever", account.password)
        self.assertEqual(self.event_config, account.event_config)
        self.assertEqual(self.storage_config, account.storage_config)
        self.assertEqual(self.workspace_config, account.workspace_config)

        self.assertEqual(3, len(account.workspaces))

        self.assertEqual(1, account.workspaces[0].workspace_number)
        self.assertEqual("classroom-999-001", account.workspaces[0].workspace_name)

        self.assertEqual(2, account.workspaces[1].workspace_number)
        self.assertEqual("classroom-999-002", account.workspaces[1].workspace_name)

        self.assertEqual(3, account.workspaces[2].workspace_number)
        self.assertEqual("classroom-999-003", account.workspaces[2].workspace_name)

    def test_create_account_config_region(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "region" must be a string value, found <class \'int\'>.""",
                             lambda: AccountConfig(region=0, account_id="1234", username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "region" must be a string value, found <class \'NoneType\'>.""",
                             lambda: AccountConfig(region=None, account_id="1234", username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))
        test_assertion_error(self, """The parameter "region" must be specified, found "".""",
                             lambda: AccountConfig(region="", account_id="1234", username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))

    def test_create_account_config_account_id(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "account_id" must be a string value, found <class \'int\'>.""",
                             lambda: AccountConfig(region="us-west-2", account_id=0, username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "account_id" must be a string value, found <class \'NoneType\'>.""",
                             lambda: AccountConfig(region="us-west-2", account_id=None, username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))
        test_assertion_error(self, """The parameter "account_id" must be specified, found "".""",
                             lambda: AccountConfig(region="us-west-2", account_id="", username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))

    def test_create_account_config_username(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "username" must be a string value, found <class \'int\'>.""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username=0, password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "username" must be a string value, found <class \'NoneType\'>.""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username=None, password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))
        test_assertion_error(self, """The parameter "username" must be specified, found "".""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username="", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))

    def test_create_account_password(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "password" must be a string value, found <class \'int\'>.""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password=0, event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))
        # noinspection PyTypeChecker
        test_assertion_error(self, """The parameter "password" must be a string value, found <class \'NoneType\'>.""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password=None, event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))
        test_assertion_error(self, """The parameter "password" must be specified, found "".""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password="", event_config=self.event_config, storage_config=self.storage_config, workspace_config=self.workspace_config))

    def test_create_account_event_config(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        test_assertion_error(self, """The parameter "event_config" must be specified.""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password="whatever", event_config=None, storage_config=self.storage_config, workspace_config=self.workspace_config))

    def test_create_account_storage_config(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        test_assertion_error(self, """The parameter "storage_config" must be specified.""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=None, workspace_config=self.workspace_config))

    def test_create_account_workspace_config(self):
        from dbacademy_test.workspaces_3_0 import test_assertion_error

        test_assertion_error(self, """The parameter "workspace_config" must be specified.""",
                             lambda: AccountConfig(region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password="whatever", event_config=self.event_config, storage_config=self.storage_config, workspace_config=None))


if __name__ == '__main__':
    unittest.main()
