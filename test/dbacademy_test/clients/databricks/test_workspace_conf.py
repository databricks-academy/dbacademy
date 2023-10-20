__all__ = ["WorkspaceConfigTests"]

import unittest
from typing import Dict, Any
from dbacademy.clients import databricks


class WorkspaceConfigTests(unittest.TestCase):

    def setUp(self) -> None:
        self.__client = databricks.from_token(scope="DBACADEMY_UNIT_TESTS")
        self.tearDown()

    @property
    def client(self):
        return self.__client

    def test_get_config_list(self):
        property_names = [
            "enable-X-Frame-Options",
            "intercomAdminConsent",
            "enableDbfsFileBrowser",
            "enableWebTerminal",
            "enableExportNotebook",
            "enableTokensConfig",
        ]
        config = self.client.workspace_config.get_config(property_names)
        self.__validate_config(config)

    def test_get_config_dict(self):
        random_config = {
            "enable-X-Frame-Options": "false",  # Turn off iframe prevention
            "intercomAdminConsent": "false",  # Turn off product welcome
            "enableDbfsFileBrowser": "true",  # Enable DBFS UI
            "enableWebTerminal": "true",  # Enable Web Terminal
            "enableExportNotebook": "true",  # We will disable this in due time
            "enableTokensConfig": "true",  # Newer courses need access to the tokens config
        }

        # Use the dictionary's keys to produce the list.
        config = self.client.workspace_config.get_config(random_config.keys())
        self.__validate_config(config)

    def test_get_config_args(self):
        config = self.client.workspace_config.get_config("enable-X-Frame-Options",
                                                         "intercomAdminConsent",
                                                         "enableDbfsFileBrowser",
                                                         "enableWebTerminal",
                                                         "enableExportNotebook",
                                                         "enableTokensConfig",)
        self.__validate_config(config)

    def __validate_config(self, config: Dict[str, Any]):
        self.assertIsNotNone(config)
        self.assertEqual(6, len(config.keys()))

        self.assertTrue("enable-X-Frame-Options" in config.keys())
        self.assertTrue("intercomAdminConsent" in config.keys())
        self.assertTrue("enableDbfsFileBrowser" in config.keys())
        self.assertTrue("enableWebTerminal" in config.keys())
        self.assertTrue("enableExportNotebook" in config.keys())
        self.assertTrue("enableTokensConfig" in config.keys())

        self.assertIsNone(config.get("enable-X-Frame-Options"))
        self.assertIsNone(config.get("intercomAdminConsent"))
        self.assertIsNone(config.get("enableDbfsFileBrowser"))
        self.assertTrue(config.get("enableExportNotebook") in [True, "true", "True", False, "false", "False"])
        self.assertTrue(config.get("enableWebTerminal") in [True, "true", "True", False, "false", "False"])

    def test_patch_config(self):
        # Change all the values to false
        self.client.workspace_config.patch_config({
            "enableExportNotebook": False,
            "enableWebTerminal": False,
        })

        # Get the config and assert they were set to False
        config = self.client.workspace_config.get_config("enableWebTerminal", "enableExportNotebook")
        self.assertIsNotNone(config)
        self.assertEqual(2, len(config.keys()))
        self.assertEqual("false", config.get("enableExportNotebook"))
        self.assertEqual("false", config.get("enableWebTerminal"))

        # Change all the values to true
        self.client.workspace_config.patch_config({
            "enableExportNotebook": "true",
            "enableWebTerminal": "true",
        })

        # Get the config and assert they were set to True
        config = self.client.workspace_config.get_config("enableWebTerminal", "enableExportNotebook")
        self.assertIsNotNone(config)
        self.assertEqual(2, len(config.keys()))
        self.assertEqual("true", config.get("enableExportNotebook"))
        self.assertEqual("true", config.get("enableWebTerminal"))


if __name__ == '__main__':
    unittest.main()
