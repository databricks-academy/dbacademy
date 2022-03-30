# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient


class PipelinesClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

        self.base_uri = f"{self.endpoint}/api/2.0/pipelines"

    def list(self):
        pipelines = []
        response = self.client.execute_get_json(f"{self.base_uri}")
        
        next_page_token = response.get("next_page_token")
        pipelines.extend(response.get("statuses"))

        while next_page_token is not None:
            next_page_token = response.get("next_page_token")
            pipelines.extend(response.get("statuses"))

        return pipelines

    # def list_events_by_id(self):
    #     return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}/events")

    # def list_events_by_id(self):
    #     return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}/events")

    def get_by_id(self, pipeline_id):
        return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}")

    def get_by_name(self, pipeline_name):
        for pipeline in self.list():
            if pipeline.get("name") == pipeline_name:
                return pipeline
        return None

    # def get_updates_by_id(self, pipeline_id, update_id):
    #     return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}/updates/{update_id}")

    def delete_by_id(self, pipeline_id):
        return self.client.execute_delete_json(f"{self.base_uri}/{pipeline_id}")

    def delete_by_name(self, pipeline_name):
        import time
        pipeline = self.get_by_name(pipeline_name)
        if pipeline is not None:
            response = self.delete_by_id(pipeline.get("pipeline_id"))

        while self.get_by_name(pipeline_name) is not None:
            time.sleep(5) # keep blocking until it's gone.

        return response

    def existing_to_create(self, pipeline):
        assert type(pipeline) == dict, f"Expected the \"pipeline\" parameter to be of type dict, found {type(pipeline)}"

        spec = pipeline.get("spec")
        del spec["id"]
        
        return spec

    def create_from_dict(self, params:dict):
        return self.client.execute_post_json(f"{self.base_uri}", params)

    def create(self, name:str, storage:str, target:str, continuous:bool=False, development:bool=True, configuration:dict = None, notebooks=None, libraries:list=None, clusters:list = None):
        
        if configuration is None:
            configuration = {}
        assert type(configuration) == dict, f"Expected configuration to be of type dict, found {type(configuration)}"
        

        if clusters == None: clusters = []
        assert type(clusters) == list, f"Expected clusters to be of type list, found {type(clusters)}"
        if len(clusters) == 0:
            clusters.append({
                "num_workers": 1
            })    

        if notebooks is not None:
            libraries = []
            for notebook in notebooks:
                libraries.append({
                    "notebook": {
                        "path": notebook
                    }    
                })

        assert type(libraries) == list, f"Expected libraries to be of type list, found {type(libraries)}"
        for library in libraries:
            notebook = library.get("notebook")
            assert notebook is not None, f"The library's notebook parameter must be specified."
            
            path = notebook.get("path")
            assert path is not None, f"The library's notebook's path parameter must be specified."
        
        params = dict()
        params["name"] = name
        params["storage"] = storage
        params["configuration"] = configuration
        params["clusters"] = clusters
        params["libraries"] = libraries
        params["target"] = target
        params["continuous"] = continuous
        params["development"] = development

        return self.create_from_dict(params)
