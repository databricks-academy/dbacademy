import unittest
from dbacademy.dbhelper import DBAcademyHelper


class TestDBAcademyHelper(unittest.TestCase):
    pass

    # def test_to_username_hash(self):
    #     username = "mickey.mouse@disney.com"
    #     da_name, da_hash = DBAcademyHelper.to_unique_name(username, "ex")
    #
    #     self.assertEqual("mickey.mouse", da_name)
    #     self.assertEqual(9984, da_hash)
    #
    #     username = "donald.duck@disney.com"
    #     da_name, da_hash = DBAcademyHelper.to_unique_name(username, "tst")
    #
    #     self.assertEqual("donald.duck", da_name)
    #     self.assertEqual(8192, da_hash)

    # def test_to_catalog_name(self):
    #     catalog_name = DBAcademyHelper.to_catalog_name(username="mickey.mouse@disney.com")
    #     self.assertEqual("mickey_mouse_9744_dbacademy", catalog_name)  # add assertion here
    #
    #     catalog_name = DBAcademyHelper.to_catalog_name(username="donald.duck@disney.com")
    #     self.assertEqual("donald_duck_8464_dbacademy", catalog_name)  # add assertion here
    #
    # def test_to_schema_name(self):
    #     schema_name = DBAcademyHelper.to_schema_name("mickey.mouse@disney.com", "ex")
    #     self.assertEqual("da_mickey_mouse_9984_ex", schema_name)  # add assertion here
    #
    #     schema_name = DBAcademyHelper.to_schema_name("donald.duck@disney.com", "tst")
    #     self.assertEqual("da_donald_duck_8192_tst", schema_name)  # add assertion here
    #
    #     schema_name = DBAcademyHelper.to_schema_name("donald.duck@disney.com", "tst-123")
    #     self.assertEqual("da_donald_duck_8192_tst_123", schema_name)  # add assertion here

if __name__ == '__main__':
    unittest.main()
