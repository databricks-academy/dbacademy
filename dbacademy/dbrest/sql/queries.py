from dbacademy.dbrest import DBAcademyRestClient
import builtins

class SqlQueriesClient:

    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint
        self.base_uri = f"{self.endpoint}/api/2.0/preview/sql/queries"

        self.max_page_size = 250

    def list(self, queries=None, page=1):
        if queries is None: queries = builtins.list()

        url = f"{self.base_uri}?page_size={self.max_page_size}&page={page}"
        json_response = self.client.execute_get_json(url)

        queries.extend(json_response.get("results", builtins.list()))

        if json_response.get("count") == len(queries): return queries
        else: return self.list(queries=queries, page=page+1)

    def get_by_id(self, query_id):
        return self.client.execute_get_json(f"{self.base_uri}/{query_id}")

    def get_by_name(self, query_name, queries=None, page=1):
        if queries is None: queries = builtins.list()

        url = f"{self.base_uri}?page_size={self.max_page_size}&page={page}"
        json_response = self.client.execute_get_json(url)

        queries.extend(json_response.get("results", builtins.list()))

        for query in queries:
            if query_name == query.get("name"):
                return query

        # Not found, continue looking.
        if json_response.get("count") == len(queries): return None
        else: return self.get_by_name(query_name=query_name, queries=queries, page=page+1)

    def clone(self, query:dict):
        create_def = self.existing_to_create(query)
        return self.create_from_dict(query)

    def existing_to_create(self, query:dict):
        assert type(query) == dict, f"Expected the \"query\" parameter to be of type dict, found {type(query)}"

        for key in list(query.keys()):
            if key not in ["query", "name", "description", "schedule", "options"]:
                del query[key]

        parameters = query.get("options", builtins.dict()).get("parameters", builtins.list())
        for parameter in parameters:
            del parameter["parentQueryId"]

        return query

    def create_from_dict(self, params:dict):
        return self.client.execute_post_json(f"{self.base_uri}", params)

    def create(name:str, query:str, description:str=None, schedule:dict=None, options:dict=None):
        params = dict()
        params["query"] = query
        params["name"] = name
        params["description"] = description
        params["schedule"] = schedule
        params["options"] = options
        return self.create_from_dict(params)