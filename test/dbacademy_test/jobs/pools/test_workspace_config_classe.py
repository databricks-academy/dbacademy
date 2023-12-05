import unittest
import pytest

from dbacademy.common import ValidationError
from dbacademy.jobs.pools.workspace_config_classe import WorkspaceConfig


class TestWorkspaceConfig(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self):
        self.dbc_urls = [
            "https://labs.training.databricks.com/api/courses?course=apache-spark-programming-with-databricks&version=vCURRENT&artifact=lessons.dbc&token=abcd",
            "https://labs.training.databricks.com/api/courses?course=example-course&version=vLATEST&artifact=lessons.dbc&token=abcd",
            "https://labs.training.databricks.com/api/courses?course=data-engineering-with-databricks&version=V1.2.3&artifact=lessons.dbc&token=abcd",
        ]
        self.datasets = [
            "example-course",
            "machine-learning-in-production",
        ]
        self.course_definitions = [
            "course=example-course",
            "course=machine-learning-in-production",
        ]

    def test_create_workspace_config_no_groups(self):
        from dbacademy_test.jobs.pools import test_index_error

        workspace_group = {
            "instructors": [0, 1],
            "analysts": ["analysts-1@databricks.com", "analysts-2@databricks.com"],
        }

        entitlements = {
            "allow-cluster-create": True,
            "allow-super-powers": False,
        }

        max_participants = 150
        expected_users = max_participants + 3  # script_account, analyst-1, analyst-2
        workspace = WorkspaceConfig(max_participants=max_participants,
                                    default_node_type_id="i3.xlarge",
                                    credentials_name="default",
                                    storage_configuration="us-west-2",
                                    username_pattern="class+{student_number}@databricks.com",
                                    entitlements=entitlements,
                                    workspace_group=workspace_group,
                                    workspace_name_pattern="classroom-{workspace_number}")

        self.assertEqual(expected_users, len(workspace.usernames))
        self.assertEqual("i3.xlarge", workspace.default_node_type_id)

        self.assertIsNone(workspace.workspace_number)
        self.assertIsNone(workspace.name)

        self.assertEqual("default", workspace.credentials_name)
        self.assertEqual("us-west-2", workspace.storage_configuration)

        self.assertEqual(2, len(workspace.entitlements))
        self.assertTrue("allow-cluster-create" in workspace.entitlements)
        self.assertTrue(workspace.entitlements.get("allow-cluster-create"))

        self.assertTrue("allow-super-powers" in workspace.entitlements)
        self.assertFalse(workspace.entitlements.get("allow-super-powers"))

        self.assertTrue(2, len(workspace.workspace_group))

        self.assertTrue("instructors" in workspace.workspace_group)
        instructors = workspace.workspace_group.get("instructors")
        self.assertEqual(["class+000@databricks.com", "class+001@databricks.com"], instructors)

        self.assertTrue("analysts" in workspace.workspace_group)
        analysts = workspace.workspace_group.get("analysts")
        self.assertEqual(["analysts-1@databricks.com", "analysts-2@databricks.com"], analysts)

        # Spotcheck users
        self.assertEqual(expected_users, len(workspace.usernames))
        self.assertEqual("class+000@databricks.com", workspace.usernames[0])   # The instructor
        self.assertEqual("class+020@databricks.com", workspace.usernames[20])
        self.assertEqual("class+040@databricks.com", workspace.usernames[40])
        self.assertEqual("class+060@databricks.com", workspace.usernames[60])
        self.assertEqual("class+080@databricks.com", workspace.usernames[80])
        self.assertEqual("class+100@databricks.com", workspace.usernames[100])
        self.assertEqual("class+120@databricks.com", workspace.usernames[120])
        self.assertEqual("class+149@databricks.com", workspace.usernames[149])

        self.assertEqual("class+150@databricks.com", workspace.usernames[150])

        self.assertEqual("analysts-1@databricks.com", workspace.usernames[151])  # Analyst #1
        self.assertEqual("analysts-2@databricks.com", workspace.usernames[152])  # Analyst #2

        test_index_error(self, lambda: workspace.usernames[153])

    def test_create_workspace_config_username_pattern(self):
        workspace = WorkspaceConfig(max_participants=250,
                                    default_node_type_id="i3.xlarge",
                                    credentials_name="default",
                                    storage_configuration="us-west-2",
                                    username_pattern="class+{student_number}@databricks.com",
                                    entitlements=dict(),
                                    workspace_group=None,
                                    workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual("class+{student_number}@databricks.com", workspace.username_pattern)

        for i, username in enumerate(workspace.usernames):
            if username != "class+analyst@databricks.com":
                self.assertEqual(f"class+{i:03d}@databricks.com", username)

        ###################################################

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern=0, workspace_group=None, entitlements=dict(), workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'username_pattern' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern=None, workspace_group=None, entitlements=dict(), workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'username_pattern' must be specified.", e.message)

        try:
            WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="", workspace_group=None, entitlements=dict(), workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'username_pattern' must have a minimum length of 5, found 0.", e.message)

    def test_create_workspace_config_workspace_pattern(self):

        workspace = WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual("classroom-{workspace_number}", workspace.workspace_name_pattern)

        workspace = WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual("classroom-{workspace_number}", workspace.workspace_name_pattern)

        ###################################################

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern=0)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'workspace_name_pattern' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern=None)
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'workspace_name_pattern' must be specified.", e.message)

        try:
            WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'workspace_name_pattern' must have a minimum length of 5, found 0.", e.message)

        try:
            WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{whatever}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("""Expected the parameter "workspace_name_pattern" to contain "{workspace_number}", found "classroom-{whatever}".""", e.message)

    def test_create_workspace_config_workspace_number(self):

        workspace = WorkspaceConfig(max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertIsNone(workspace.workspace_number)

        ###################################################

        workspace = WorkspaceConfig(workspace_number=1999, max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
        self.assertEqual(1999, workspace.workspace_number)

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(workspace_number="0", max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'workspace_number' to be of type <class 'int'>, found <class 'str'>.", e.message)

        try:
            WorkspaceConfig(workspace_number=0, max_participants=250, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Value | The parameter 'workspace_number' must have a minimum value of '1000', found '0'.", e.message)

    def test_create_workspace_config_max_participants(self):
        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants="0", default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'max_participants' to be of type <class 'int'>, found <class 'str'>.", e.message)

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=None, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'max_participants' must be specified.", e.message)

        try:
            WorkspaceConfig(max_participants=0, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Value | The parameter 'max_participants' must have a minimum value of '1', found '0'.", e.message)

    def test_create_workspace_config_default_node_type_id(self):

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=250, default_node_type_id=0, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'default_node_type_id' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=250, default_node_type_id=None, credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'default_node_type_id' must be specified.", e.message)

        try:
            WorkspaceConfig(max_participants=250, default_node_type_id="abc", credentials_name="default", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'default_node_type_id' must have a minimum length of 5, found 3.", e.message)

    def test_create_workspace_config_credentials_name(self):

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=150, default_node_type_id="i3.xlarge", credentials_name=0, storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'credentials_name' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=150, default_node_type_id="i3.xlarge", credentials_name=None, storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'credentials_name' must be specified.", e.message)

        try:
            WorkspaceConfig(max_participants=150, default_node_type_id="i3.xlarge", credentials_name="", storage_configuration="us-west-2", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'credentials_name' must have a minimum length of 5, found 0.", e.message)

    def test_create_workspace_config_storage_config(self):

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=150, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration=0, username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Type | Expected the parameter 'storage_configuration' to be of type <class 'str'>, found <class 'int'>.", e.message)

        try:
            # noinspection PyTypeChecker
            WorkspaceConfig(max_participants=150, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration=None, username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Not-None | The parameter 'storage_configuration' must be specified.", e.message)

        try:
            WorkspaceConfig(max_participants=150, default_node_type_id="i3.xlarge", credentials_name="default", storage_configuration="", username_pattern="class+{student_number}@databricks.com", entitlements=dict(), workspace_group=None, workspace_name_pattern="classroom-{workspace_number}")
            self.fail("Expected ValidationError")
        except ValidationError as e:
            self.assertMultiLineEqual("Error-Min-Len | The parameter 'storage_configuration' must have a minimum length of 5, found 0.", e.message)


if __name__ == '__main__':
    unittest.main()
