# Databricks notebook source
# MAGIC %pip install \
# MAGIC git+https://github.com/databricks-academy/dbacademy-gems \
# MAGIC --quiet --disable-pip-version-check

# COMMAND ----------

import unittest
import pytest

from dbacademy.clients.rest.common import ApiClient, DatabricksApiException
from dbacademy.clients.rest.factory import dougrest_factory


class TestApiClient(unittest.TestCase):
    """
    Test client error handling, retry, and backoff features.
    """

    def testApiSimple(self):
        ws = dougrest_factory.test_client()
        results = ws.api("GET", "/api/2.0/workspace/list", path="/")
        self.assertIsNotNone(results)

    def testExpected404(self):
        ws = dougrest_factory.test_client()
        results = ws.api("GET", "/api/2.0/workspace/list", path="/does-not-exist", _expected=404)
        self.assertIsNone(results)

    def testSelfCallable(self):
        ws = dougrest_factory.test_client()
        self.assertEqual(ws, ws())

    def testWithHostname(self):
        # We intentionally pass in a full URL as part of testing for legacy compatibility.
        ws = dougrest_factory.test_client()
        url = ws.endpoint + "/api/2.0/workspace/list?path=/"
        results = ws.api("GET", url)
        self.assertIsNotNone(results)

    def testWithWrongHostname(self):
        ws = dougrest_factory.test_client()
        url = "https://unknown.domain.com/api/2.0/workspace/list?path=/"
        try:
            ws.api("GET", url)
            self.fail("Expected ValueError due to wrong hostname in URL.")
        except ValueError:
            pass

    def testExecuteGetJsonExpected404(self):
        ws = dougrest_factory.test_client()
        url = "/api/2.0/workspace/list?path=/does-not-exist"
        results = ws.api("GET", url, _expected=404)
        self.assertIsNone(results)

    def testNotFound(self):
        ws = dougrest_factory.test_client()
        try:
            ws.api("GET", "does-not-exist")
            self.fail("404 DatabricksApiException expected")
        except DatabricksApiException as e:
            self.assertEqual(e.http_code, 404)

    def testUnauthorized(self):
        default_ws = dougrest_factory.test_client()
        try:
            client = ApiClient(default_ws.endpoint, token="INVALID")
            client.api("GET", "/api/2.0/workspace/list")
            self.fail("403 DatabricksApiException expected")
        except DatabricksApiException as e:
            self.assertIn(e.http_code, (401, 403))

    @pytest.mark.skip(reason="This test is flaky and fails intermittently.")
    def testThrottle(self):
        print()
        print("Ignore the next throttle warning.  It is intentionally being testing.")
        default_ws = dougrest_factory.test_client()
        ws = ApiClient(default_ws.url,
                       authorization_header=default_ws.session.headers["Authorization"],
                       throttle_seconds=1)
        import time
        t1 = time.time()
        ws.api("GET", "/api/2.0/clusters/list-node-types")
        t2 = time.time()
        ws.api("GET", "/api/2.0/clusters/list-node-types")
        t3 = time.time()
        self.assertLess(t2-t1, 1, f"t2-t1 ({t2-t1}) is less than 1")
        self.assertGreater(t3-t2, 1, f"t3-t2 ({t3-t2}) is greater than 1")


# COMMAND ----------

def main():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestApiClient))
    runner = unittest.TextTestRunner()
    runner.run(suite)


# COMMAND ----------

if __name__ == '__main__':
    main()
