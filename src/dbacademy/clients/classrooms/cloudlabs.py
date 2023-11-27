__all__ = ["MyHTMLParser"]

"""
Logic used to mine Cloudlabs lab configurations during Data and AI summit.  This logic needs refactoring.
"""
import json
import requests
import re
import urllib.parse
from html.parser import HTMLParser
from multiprocessing.pool import ThreadPool
from dbacademy.clients.dougrest import DatabricksApiClient


# TODO remove unused parameter
# noinspection PyUnusedLocal
def curl_login(self):
    from dbacademy.dbgems import dbutils
    if dbutils:
        return dbutils.secrets.get("admin", "doug-azure")


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
# TODO Doug, I don't know how to address this one - or at least I don't want to invest the time :-p
# noinspection PyTypeChecker
cookies = re.search("'Cookie: ([^']*)'", curl_login)[1].strip().split("; ")
# noinspection PyTypeChecker
cookies = dict(c.split("=", maxsplit=1) for c in cookies)
for key, value in cookies.items():
    web.cookies.set(key, value, domain="login.microsoftonline.com", path="/")

cloudlabs_login = "https://cloudlabsai.b2clogin.com/cloudlabsai.onmicrosoft.com/b2c_1a_signup_signin_linkedin"
response = web.get(f"{cloudlabs_login}/oauth2/v2.0/authorize", params={
  'response_type': 'id_token',
  'scope': 'openid profile',
  'client_id': 'e92e446f-5d92-4100-8c37-7e31fbd69c04',
  'redirect_uri': 'https://admin.cloudlabs.ai/',
  'state': 'c3ec0a0c-c3ab-4545-b1c9-728e0c940078',
  'nonce': 'a0c1ff7e-7a3f-46a6-b284-6bb91a09d98e',
  'client_info': '1',
  'x-client-SKU': 'MSAL.JS',
  'x-client-Ver': '0.2.1',
  'client-request-id': '0397029e-b2a0-434e-b1fd-dc8a7b9d8a91',
  'prompt': 'select_account',
  'response_mode': 'fragment'
})

# TODO remove redundancy
# noinspection RegExpRedundantEscape
settings_json = re.search(r'var SETTINGS = ({.*"retryOn":\["error","timeout"\]}}).*', response.text)[1]
settings_json = re.sub(r"{allowedTags.*indexOf\('IMPORT'\) >= 0\)}}", '""', settings_json)
settings = json.loads(settings_json)
response = web.get(f"{cloudlabs_login}/api/CombinedSigninAndSignup/unified", params={
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
web.headers["Authorization"] = "Bearer " + token
web.headers['roleid'] = '43375134326F4F7130564F3748766963514E48574E413D3D'
web.headers['tenantid'] = '3279416D4E696563426D7048637545563564676733413D3D'

data = json.loads("""{"State":"1","InstructorId":null,"StartIndex":1000,"PageCount":1}""")
labs = web.post("https://api.cloudlabs.ai/api/OnDemandLab/GetOnDemandLabs", json=data).json()
summit_aws = [lab for lab in labs if lab['Title'].startswith("AWS-")]
summit_msa = [lab for lab in labs if lab['Title'].startswith("Azure-")]
summit_gcp = [lab for lab in labs if lab['Title'].startswith("GCP-")]
summit_all = summit_aws+summit_msa+summit_gcp

pattern = re.compile("^([^-]*)-([^|]*[^ |])( *|.*)$")
labs = {}
for course in summit_all:
    match = pattern.match(course["Title"])
    course["Cloud"] = match[1]
    course["Course Title"] = match[2]
    course["Long Title"] = match[2] + match[3]
    labs.setdefault(course["Long Title"], {})[course["Cloud"]] = course
lab_titles = labs.keys()

all_labs = [lab for group in labs.values() for lab in group.values()]


def lookup_workspaces(lab):
    odl_id = lab["Id"]
    put_response = web.put(f"https://cloudlabs-api-s1-cu.azurewebsites.net/api/Export/GetDatabricksWorkspace?eventId={odl_id}")
    if '"Status":"Error"' in put_response.text:
        lab["Workspaces"] = []
        return lab
    import csv
    from io import StringIO
    rows = [line for line in csv.reader(StringIO(put_response.text))]
    for i, row in enumerate(rows):
        for j, col in enumerate(row):
            row[j] = col.strip()
    header = rows[0]
    workspaces = []
    for row in rows[1:]:
        assert len(row) == len(header)
        row = dict(zip(header, row))
        row["Workspace ID"] = row["Id"]
        del row["Id"]
        row["API"] = DatabricksApiClient(hostname=row["Url"][8:], token=row["Token"], deployment_name=lab["Title"])
        row["Long Title"] = lab["Long Title"]
        row["Cloud"] = lab["Cloud"]
        workspaces.append(row)
    lab["Workspaces"] = workspaces
    return lab


with ThreadPool(len(all_labs)) as pool:
    pool.map(lookup_workspaces, all_labs)

summit_workspaces = [workspace["API"] for lab in all_labs for workspace in lab["Workspaces"]]
