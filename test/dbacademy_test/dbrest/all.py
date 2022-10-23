

def main():
    import unittest
    from dbacademy_test.dbrest.jacob_client import TestDBAcademyRestClient as TestDBRest
    from dbacademy_test.dbrest.highlevel import TestHighLevelFeatures
    from dbacademy_test.dbrest.api_client import TestApiClient
    from dbacademy_test.dbrest.accounts import TestAccountsApi
    from dbacademy_test.dbrest.permissions import TestPermissionsApi
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
