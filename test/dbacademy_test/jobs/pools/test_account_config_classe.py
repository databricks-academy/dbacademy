import unittest
import pytest

from dbacademy.common import ValidationError
from dbacademy.jobs.pools.account_config_class import AccountConfig
from dbacademy.jobs.pools.uc_storage_config_class import UcStorageConfig
from dbacademy.jobs.pools.workspace_config_classe import WorkspaceConfig


class TestAccountConfig(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self):
        self.uc_storage_config = UcStorageConfig(storage_root="def", storage_root_credential_id="ghi", region="jkl", meta_store_owner="instructors", aws_iam_role_arn="abc", msa_access_connector_id=None)

        entitlements = {
            "allow-cluster-create": False,  # Removed to enforce policy
            "databricks-sql-access": True,
            "workspace-access": True,
        }

        self.workspace_config = WorkspaceConfig(max_participants=100, default_node_type_id="i3.xlarge", default_dbr="11.3.x-scala2.12", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=entitlements, workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")

    def test_from_env(self):
        env_id = "CURR"
        account_id_env_name = f"WORKSPACE_SETUP_{env_id}_ACCOUNT_ID"      # Loaded from the environment to make
        account_password_env_name = f"WORKSPACE_SETUP_{env_id}_PASSWORD"  # configuration easily swapped between systems
        account_username_env_name = f"WORKSPACE_SETUP_{env_id}_USERNAME"  # Refactor to use .databricks/profile-name/etc.

        account = AccountConfig.from_env(account_id_env_name=account_id_env_name,
                                         account_password_env_name=account_password_env_name,
                                         account_username_env_name=account_username_env_name,
                                         workspace_numbers=[1001, 1002, 1003],
                                         region="us-west-2",
                                         uc_storage_config=self.uc_storage_config,
                                         workspace_config_template=self.workspace_config)
        self.assertIsNotNone(account)

        self.assertEqual("b6e87bd6-c65c-46d3-abde-6840bf706d39", account.account_id)
        self.assertEqual("class+curriculum@databricks.com", account.username)
        self.assertTrue(account.password.startswith("xR"))
        self.assertTrue(account.password.endswith("gx"))

    def test_create_account_config(self):
        
        account = AccountConfig(workspace_numbers=[1003, 1004, 1005],
                                region="us-west-2",
                                account_id="asdf",
                                username="mickey.mouse@disney.com",
                                password="whatever",
                                uc_storage_config=self.uc_storage_config,
                                workspace_config_template=self.workspace_config)

        self.assertEqual("us-west-2", account.region)
        self.assertEqual("asdf", account.account_id)
        self.assertEqual("mickey.mouse@disney.com", account.username)
        self.assertEqual("whatever", account.password)
        self.assertEqual(self.uc_storage_config, account.uc_storage_config)
        self.assertEqual(self.workspace_config, account.workspace_config_template)
        self.assertEqual(0, len(account.ignored_workspaces))

        self.assertEqual(3, len(account.workspaces))

        self.assertEqual(1003, account.workspaces[0].workspace_number)
        self.assertEqual("classroom-1003-23avl", account.workspaces[0].name)

        self.assertEqual(1004, account.workspaces[1].workspace_number)
        self.assertEqual("classroom-1004-n80ls", account.workspaces[1].name)

        self.assertEqual(1005, account.workspaces[2].workspace_number)
        self.assertEqual("classroom-1005-urcq1", account.workspaces[2].name)

        # Test with different class name template

        workspace_config = WorkspaceConfig(max_participants=100,
                                           default_node_type_id="i3.xlarge",
                                           default_dbr="11.3.x-scala2.12",
                                           credentials_name="default",
                                           storage_configuration="us-west-2",
                                           username_pattern="class+{student_number}@databricks.com",
                                           entitlements={},
                                           workspace_group=None,
                                           workspace_name_pattern="Random-{workspace_number}")

        account = AccountConfig(workspace_numbers=[1007, 1008, 1009],
                                region="us-west-2",
                                account_id="asdf",
                                username="mickey.mouse@disney.com",
                                password="whatever",
                                uc_storage_config=self.uc_storage_config,
                                workspace_config_template=workspace_config)

        self.assertEqual(3, len(account.workspaces))
        self.assertEqual(1007, account.workspaces[0].workspace_number)
        self.assertEqual("random-1007-p46aq", account.workspaces[0].name)

    def test_create_account_config_ignored_workspaces(self):

        # None
        account = AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007],
                                region="us-west-2",
                                account_id="asdf",
                                username="mickey.mouse@disney.com",
                                password="whatever",
                                uc_storage_config=self.uc_storage_config,
                                workspace_config_template=self.workspace_config)
        self.assertEqual(0, len(account.ignored_workspaces))

        # Empty
        account = AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007],
                                region="us-west-2",
                                account_id="asdf",
                                username="mickey.mouse@disney.com",
                                password="whatever",
                                uc_storage_config=self.uc_storage_config,
                                workspace_config_template=self.workspace_config,
                                ignored_workspaces=[])
        self.assertEqual(0, len(account.ignored_workspaces))

        # One Int
        account = AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="asdf", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config, ignored_workspaces=[1])
        self.assertEqual(1, len(account.ignored_workspaces))
        self.assertEqual("1", account.ignored_workspaces[0])

        # Two ints
        account = AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="asdf", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config, ignored_workspaces=[1, 2])
        self.assertEqual(2, len(account.ignored_workspaces))
        self.assertEqual("1", account.ignored_workspaces[0])
        self.assertEqual("2", account.ignored_workspaces[1])

        # One String
        account = AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="asdf", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config, ignored_workspaces=["apple"])
        self.assertEqual(1, len(account.ignored_workspaces))
        self.assertEqual("apple", account.ignored_workspaces[0])

        # Two Strings
        account = AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="asdf", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config, ignored_workspaces=["apple", "banana"])
        self.assertEqual("apple", account.ignored_workspaces[0])
        self.assertEqual("banana", account.ignored_workspaces[1])

        # Ints & Strings
        account = AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="asdf", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config, ignored_workspaces=[1, "apple"])
        self.assertEqual(2, len(account.ignored_workspaces))
        self.assertEqual("1", account.ignored_workspaces[0])
        self.assertEqual("apple", account.ignored_workspaces[1])

        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="asdf", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config, ignored_workspaces=1.0)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""The parameter "ignored_workspaces" must be of type <class 'list'>, found <class 'float'>.""", e.message)

    def test_create_account_config_region(self):
        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region=0, account_id="1234", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("Error-Type | Expected the parameter 'region' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region=None, account_id="1234", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("Error-Min-Len | The parameter 'region' must have a minimum length of 1, found 0.", e.message)

        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="", account_id="1234", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("Error-Min-Len | The parameter 'region' must have a minimum length of 1, found 0.", e.message)

    def test_create_account_config_account_id(self):
        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id=0, username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""Error-Type | Expected the parameter 'account_id' to be of type <class 'str'>, found <class 'int'>.""", e.message)

        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id=None, username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""Error-Not-None | The parameter 'account_id' must be specified.""", e.message)

        try:
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""Error-Min-Len | The parameter 'account_id' must have a minimum length of 1, found 0.""", e.message)

    def test_create_account_config_username(self):
        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="1234", username=0, password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""a""", e.message)

        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="1234", username=None, password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""b""", e.message)

        try:
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="1234", username="", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""c""", e.message)

    def test_create_account_password(self):
        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password=0, uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""x""", e.message)

        try:
            # noinspection PyTypeChecker
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password=None, uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""x""", e.message)

        try:
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password="", uc_storage_config=self.uc_storage_config, workspace_config_template=self.workspace_config)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertEqual("""x""", e.message)

    def test_create_account_storage_config(self):
        try:
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=None, workspace_config_template=self.workspace_config)
        except ValidationError as e:
            self.assertEqual("""x""", e.message)

    def test_create_account_workspace_config(self):
        try:
            AccountConfig(workspace_numbers=[1001, 1002, 1003, 1004, 1005, 1006, 1007], region="us-west-2", account_id="1234", username="mickey.mouse@disney.com", password="whatever", uc_storage_config=self.uc_storage_config, workspace_config_template=None)
        except ValidationError as e:
            self.assertEqual("""x""", e.message)


if __name__ == '__main__':
    unittest.main()
