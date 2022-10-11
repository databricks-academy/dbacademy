

def main():
    import unittest
    from dbacademy.dbrest.tests.highlevel import TestHighLevelFeatures as TestDBRest
    from dbacademy.dougrest.tests.highlevel import TestHighLevelFeatures
    from dbacademy.dougrest.tests.client import TestApiClient
    from dbacademy.dougrest.tests.accounts import TestAccountsApi
    from dbacademy.dougrest.tests.permissions import TestPermissionsApi
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDBRest))
    suite.addTest(unittest.makeSuite(TestHighLevelFeatures))
    suite.addTest(unittest.makeSuite(TestApiClient))
    suite.addTest(unittest.makeSuite(TestAccountsApi))
    suite.addTest(unittest.makeSuite(TestPermissionsApi))
    runner = unittest.TextTestRunner()
    runner.run(suite)


if __name__ == '__main__':
    main()
