def main() -> None:
    """
    Run all tests.
    """
    from dbacademy.dbrest.tests.highlevel import TestHighLevelFeatures
    import unittest
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestHighLevelFeatures))
    runner = unittest.TextTestRunner()
    runner.run(suite)


if __name__ == '__main__':
    main()
