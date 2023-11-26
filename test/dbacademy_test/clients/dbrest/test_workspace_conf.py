__all__ = ["WorkspaceConfigTests"]

import unittest
from typing import Dict, Any
from dbacademy.clients import dbrest
from dbacademy_test.clients.dbrest import DBACADEMY_UNIT_TESTS

enable_web_terminal = "enableWebTerminal"
enable_export_notebook = "enableExportNotebook"

enable_x_frame_options = "enable-X-Frame-Options"
intercom_admin_consent = "intercomAdminConsent"
enable_dbfs_file_browser = "enableDbfsFileBrowser"
enable_tokens_config = "enableTokensConfig"


class WorkspaceConfigTests(unittest.TestCase):

    def setUp(self) -> None:
        self.__client = dbrest.from_token(scope=DBACADEMY_UNIT_TESTS)
        self.tearDown()

    @property
    def client(self):
        return self.__client

    def test_get_config_list(self):
        property_names = [
            enable_x_frame_options,
            intercom_admin_consent,
            enable_dbfs_file_browser,
            enable_web_terminal,
            enable_export_notebook,
            enable_tokens_config,
        ]
        config = self.client.workspace_config.get_config(property_names)
        self.__validate_config(config)

    def test_get_config_dict(self):
        random_config = {
            enable_x_frame_options: "false",  # Turn off iframe prevention
            intercom_admin_consent: "false",  # Turn off product welcome
            enable_dbfs_file_browser: "true",  # Enable DBFS UI
            enable_web_terminal: "true",  # Enable Web Terminal
            enable_export_notebook: "true",  # We will disable this in due time
            enable_tokens_config: "true",  # Newer courses need access to the tokens config
        }

        # Use the dictionary's keys to produce the list.
        config = self.client.workspace_config.get_config(random_config.keys())
        self.__validate_config(config)

    def test_get_config_args(self):
        config = self.client.workspace_config.get_config(enable_x_frame_options,
                                                         intercom_admin_consent,
                                                         enable_dbfs_file_browser,
                                                         enable_web_terminal,
                                                         enable_export_notebook,
                                                         enable_tokens_config,)
        self.__validate_config(config)

    def __validate_config(self, config: Dict[str, Any]):
        self.assertIsNotNone(config)
        self.assertEqual(6, len(config.keys()))

        self.assertTrue(enable_x_frame_options in config.keys())
        self.assertTrue(intercom_admin_consent in config.keys())
        self.assertTrue(enable_dbfs_file_browser in config.keys())
        self.assertTrue(enable_web_terminal in config.keys())
        self.assertTrue(enable_export_notebook in config.keys())
        self.assertTrue(enable_tokens_config in config.keys())

        self.assertIsNone(config.get(enable_x_frame_options))
        self.assertIsNone(config.get(intercom_admin_consent))
        self.assertIsNone(config.get(enable_dbfs_file_browser))
        self.assertTrue(config.get(enable_export_notebook) in [True, "true", "True", False, "false", "False"])
        self.assertTrue(config.get(enable_web_terminal) in [True, "true", "True", False, "false", "False"])

    def test_patch_config(self):
        # Change all the values to false
        self.client.workspace_config.patch_config({
            enable_export_notebook: False,
            enable_web_terminal: False,
        })

        # Get the config and assert they were set to False
        config = self.client.workspace_config.get_config(enable_web_terminal, enable_export_notebook)
        self.assertIsNotNone(config)
        self.assertEqual(2, len(config.keys()))

        actual = config.get(enable_export_notebook)
        self.assertEqual("false", actual, f"Found {actual}")

        actual = config.get(enable_web_terminal)
        self.assertEqual("false", actual, f"Found {actual}")

        # Change all the values to true
        self.client.workspace_config.patch_config({
            enable_export_notebook: "true",
            enable_web_terminal: "true",
        })

        # Get the config and assert they were set to True
        config = self.client.workspace_config.get_config(enable_web_terminal, enable_export_notebook)
        self.assertIsNotNone(config)
        self.assertEqual(2, len(config.keys()))

        actual = config.get(enable_export_notebook)
        self.assertEqual("true", actual, f"Found {actual}")

        actual = config.get(enable_web_terminal)
        self.assertEqual("true", actual, f"Found {actual}")


if __name__ == '__main__':
    unittest.main()
