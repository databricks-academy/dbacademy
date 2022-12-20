import unittest


class TestCloud(unittest.TestCase):

    def test_is_aws(self):
        from dbacademy.common import Cloud

        self.assertTrue(Cloud.AWS.is_aws(Cloud.AWS))
        self.assertTrue(Cloud.AWS.is_aws("AWS"))

        self.assertFalse(Cloud.AWS.is_aws(Cloud.MSA))
        self.assertFalse(Cloud.AWS.is_aws("MSA"))

        self.assertFalse(Cloud.AWS.is_aws(Cloud.GCP))
        self.assertFalse(Cloud.AWS.is_aws("GCP"))

    def test_is_msa(self):
        from dbacademy.common import Cloud

        self.assertFalse(Cloud.AWS.is_msa(Cloud.AWS))
        self.assertFalse(Cloud.AWS.is_msa("AWS"))

        self.assertTrue(Cloud.AWS.is_msa(Cloud.MSA))
        self.assertTrue(Cloud.AWS.is_msa("MSA"))

        self.assertFalse(Cloud.AWS.is_msa(Cloud.GCP))
        self.assertFalse(Cloud.AWS.is_msa("GCP"))

    def test_is_gcp(self):
        from dbacademy.common import Cloud

        self.assertFalse(Cloud.AWS.is_gcp(Cloud.AWS))
        self.assertFalse(Cloud.AWS.is_gcp("AWS"))

        self.assertFalse(Cloud.AWS.is_gcp(Cloud.MSA))
        self.assertFalse(Cloud.AWS.is_gcp("MSA"))

        self.assertTrue(Cloud.AWS.is_gcp(Cloud.GCP))
        self.assertTrue(Cloud.AWS.is_gcp("GCP"))

    def test_current(self):
        from dbacademy.common import Cloud

        self.assertEquals(Cloud.UNKNOWN, Cloud.current())


if __name__ == '__main__':
    unittest.main()
