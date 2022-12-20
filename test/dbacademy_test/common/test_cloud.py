import unittest
from dbacademy.common import Cloud


class TestCloud(unittest.TestCase):

    def test_is_aws(self):
        self.assertTrue(Cloud.AWS.is_aws)
        self.assertFalse(Cloud.MSA.is_aws)
        self.assertFalse(Cloud.GCP.is_aws)

    def test_is_msa(self):
        self.assertFalse(Cloud.AWS.is_msa)
        self.assertTrue(Cloud.MSA.is_msa)
        self.assertFalse(Cloud.GCP.is_msa)

    def test_is_gcp(self):
        self.assertFalse(Cloud.AWS.is_gcp)
        self.assertFalse(Cloud.MSA.is_gcp)
        self.assertTrue(Cloud.GCP.is_gcp)

    def test_current(self):
        self.assertEquals(Cloud.UNKNOWN, Cloud.current_cloud())


if __name__ == '__main__':
    unittest.main()
