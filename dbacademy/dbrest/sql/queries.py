from dbacademy.dbrest import DBAcademyRestClient
import builtins

class SqlQueriesClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.base_uri = f"{self.endpoint}/api/2.0/preview/sql/queries"

    def list(self, queries=None, page=1):
        if queries is None: queries = builtins.list()
        max_page_size = 250
        print(f"Size: {len(queries)}")
        url = f"{self.base_uri}?page_size={max_page_size}&page={len(queries)+1}"
        print(f"Fetching: {url}")
        json_response = self.client.execute_get_json(url)

        queries.extend(json_response.get("results", builtins.list()))

        if json_response.get("count") == len(queries): return queries
        else: return self.list(queries=queries, page=page+1)

    def get_by_id(self, query_id):
        return self.client.execute_get_json(f"{self.base_uri}/{query_id}")
