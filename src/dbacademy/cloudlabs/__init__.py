from __future__ import annotations

from functools import cached_property
from typing import Dict

from dbacademy.rest.common import ApiClient

__all__ = ["CloudlabsApi", "Tenant"]


class Tenant(ApiClient):
    def __init__(self, cloudlabs: CloudlabsApi, tenant: Dict):
        super().__init__(cloudlabs.url, token=cloudlabs.token)
        self.__dict__.update(tenant)
        self.cloudlabs = cloudlabs
        self.name = tenant["Name"]
        self.session.headers['roleid'] = tenant["InternalRoleId"]
        self.session.headers['tenantid'] = tenant["InternalPartnerId"]
        from dbacademy.cloudlabs.instructors import Instructors
        self.instructors = Instructors(self)
        from dbacademy.cloudlabs.labs import Labs
        self.labs = Labs(self)
        from dbacademy.cloudlabs.templates import Templates
        self.templates = Templates(self)

    @property
    def threadpool(self):
        return self.cloudlabs.threadpool

    def do_batch(self, f, items):
        return self.threadpool.map(f, items)


class CloudlabsApi(ApiClient):
    def __init__(self, token):
        super().__init__("https://api.cloudlabs.ai", token=token)
        from multiprocessing.pool import ThreadPool
        self.threadpool = ThreadPool(100)

    @cached_property
    def tenants(self) -> Dict[str, Tenant]:
        menu = self.api("GET", "/api/Menu/525A6E337A46535250527246334353504C355A7449673D3D")
        return {t["Name"]: Tenant(self, t) for t in menu["AssociatedTenants"]}

    @staticmethod
    def curl_auth(curl_login: str) -> CloudlabsApi:
        """
        The curl parameter should be copied from Chrome using the following steps:
        * Login to Cloudlabs
        * Open up a new Chrome
        * In the Chrome ... menu, go to "More Tools -> Developer Tools"
        * Click on the "Network" tab in the Developer Tools.
        * Visit https://login.microsoftonline.com/login.srf
        * Right-click on the first logged network entry and select "Copy -> Copy and cURL"
        Use that value as the curl parameter to pass to this login function.
        """
        import re
        import requests
        import json
        import urllib.parse
        from html.parser import HTMLParser

        class MyHTMLParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.params = []
                self.action = None
                self.method = "GET"

            def handle_starttag(self, tag, attrs):
                attrs = dict(attrs)
                if tag == "form":
                    self.method = attrs["method"]
                    self.action = attrs["action"]
                if tag == "input" and "name" in attrs:
                    self.params.append((attrs["name"], attrs["value"]))

        web = requests.Session()
        web.headers['User-Agent'] = 'Mozilla/5.0'
        cookies = re.search(r"^  -H 'Cookie: ([^']*)' \\$", curl_login, re.MULTILINE)[1].strip().split("; ")
        cookies = dict(c.split("=", maxsplit=1) for c in cookies)
        for key, value in cookies.items():
            web.cookies.set(key, value, domain="login.microsoftonline.com", path="/")

        response = web.get(
            "https://cloudlabsai.b2clogin.com/cloudlabsai.onmicrosoft.com/b2c_1a_signup_signin_linkedin/oauth2/v2.0/authorize",
            params={
                'response_type': 'id_token',
                'scope': 'openid profile',
                'client_id': 'e92e446f-5d92-4100-8c37-7e31fbd69c04',
                'redirect_uri': 'https://admin.cloudlabs.ai/',
                'state': '28bded96-d64d-4305-b7b3-2eb33e69ea97',
                'nonce': '99f223c3-e96c-479a-a44d-87332638505c',
                'client_info': '1',
                'x-client-SKU': 'MSAL.JS',
                'x-client-Ver': '0.2.1',
                'client-request-id': 'b7d6dd3e-49ff-4684-afb1-7729ceefc944',
                'prompt': 'select_account',
                'response_mode': 'fragment'
            })

        settings_json = re.search(r'var SETTINGS = ({.*"retryOn":\["error","timeout"\]}}).*', response.text)[1]
        settings_json = re.sub(r"{allowedTags.*indexOf\('IMPORT'\) >= 0\)}}", '""', settings_json)
        settings = json.loads(settings_json)
        response = web.get(
            "https://cloudlabsai.b2clogin.com/cloudlabsai.onmicrosoft.com/B2C_1A_signup_signin_linkedin/api/CombinedSigninAndSignup/unified",
            params={
                'social': 'AzureADExchange',
                'csrf_token': settings["csrf"],
                'tx': settings["transId"],
                'p': 'B2C_1A_signup_signin_linkedin',
            })

        parser = MyHTMLParser()
        parser.feed(response.text)
        response = web.request(parser.method, url=parser.action, params=parser.params, allow_redirects=True)
        location = response.history[-1].headers["Location"]
        token = urllib.parse.parse_qs(urllib.parse.urlparse(location).fragment)["id_token"][0]
        return CloudlabsApi(token)
