import unittest
from dbacademy.dbhelper import DBAcademyHelper


class TestDBAcademyHelper(unittest.TestCase):

    def test_to_username_hash(self):
        username = "mickey.mouse@disney.com"
        da_name, da_hash = DBAcademyHelper.to_username_hash(username, "ex")

        self.assertEqual("mickey.mouse", da_name)
        self.assertEqual(9984, da_hash)

        username = "donald.duck@disney.com"
        da_name, da_hash = DBAcademyHelper.to_username_hash(username, "tst")

        self.assertEqual("donald.duck", da_name)
        self.assertEqual(8192, da_hash)


if __name__ == '__main__':
    unittest.main()
