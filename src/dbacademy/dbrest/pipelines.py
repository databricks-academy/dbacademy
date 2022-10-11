from dbacademy.dbrest import DBAcademyRestClient
import builtins
from typing import Union

from dbacademy.rest.common import ApiContainer


class PipelinesClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client
        self.base_uri = f"{self.client.endpoint}/api/2.0/pipelines"

    def list(self, max_results=100):
        pipelines = []
        response = self.client.execute_get_json(f"{self.base_uri}?max_results={max_results}")

        while True:
            pipelines.extend(response.get("statuses", builtins.list()))
            next_page_token = response.get("next_page_token")
            if next_page_token is None:
                break
            response = self.client.execute_get_json(f"{self.base_uri}?max_results={max_results}&page_token={next_page_token}")

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
                return self.get_by_id(pipeline.get("pipeline_id"))
        return None

    def get_update_by_id(self, pipeline_id, update_id):
        return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}/updates/{update_id}")

    def delete_by_id(self, pipeline_id):
        return self.client.execute_delete_json(f"{self.base_uri}/{pipeline_id}")

    def delete_by_name(self, pipeline_name):
        import time
        pipeline = self.get_by_name(pipeline_name)
        response = None if pipeline is None else self.delete_by_id(pipeline.get("pipeline_id"))

        while self.get_by_name(pipeline_name) is not None:
            time.sleep(5)  # keep blocking until it's gone.

        return response

    @staticmethod
    def existing_to_create(pipeline):
        assert type(pipeline) == dict, f"Expected the \"pipeline\" parameter to be of type dict, found {type(pipeline)}"

        spec = pipeline.get("spec")
        del spec["id"]
        
        return spec

    def update_from_dict(self, pipeline_id: str, params: dict):
        return self.client.execute_put_json(f"{self.base_uri}/{pipeline_id}", params)

    def create_from_dict(self, params: dict):
        return self.client.execute_post_json(f"{self.base_uri}", params)

    def create_or_update(self, name: str, storage: str, target: str, continuous: bool = False, development: bool = True, configuration: dict = None, notebooks: list = None, libraries: list = None, clusters: list = None, min_workers: int = 0, max_workers: int = 0, photon: bool = True, pipeline_id: Union[str, None] = None):

        params = self.to_dict(name=name,
                              storage=storage,
                              target=target,
                              continuous=continuous,
                              development=development,
                              configuration=configuration,
                              notebooks=notebooks,
                              libraries=libraries,
                              clusters=clusters,
                              min_workers=min_workers,
                              max_workers=max_workers,
                              photon=photon)

        if pipeline_id is None:
            pipeline = self.get_by_name(name)
            if pipeline is not None:
                pipeline_id = pipeline.get("pipeline_id")
                self.update_from_dict(pipeline_id, params)
                return pipeline_id

        return self.create_from_dict(params).get("pipeline_id")

    def update(self, pipeline_id: str, name: str, storage: str, target: str, continuous: bool = False, development: bool = True, configuration: dict = None, notebooks: list = None, libraries: list = None, clusters: list = None, min_workers: int = 0, max_workers: int = 0, photon: bool = True):
        params = self.to_dict(name=name,
                              storage=storage,
                              target=target,
                              continuous=continuous,
                              development=development,
                              configuration=configuration,
                              notebooks=notebooks,
                              libraries=libraries,
                              clusters=clusters,
                              min_workers=min_workers,
                              max_workers=max_workers,
                              photon=photon)
        return self.update_from_dict(pipeline_id, params)

    def create(self, name: str, storage: str, target: str, continuous: bool = False, development: bool = True, configuration: dict = None, notebooks: list = None, libraries: list = None, clusters: list = None, min_workers: int = 0, max_workers: int = 0, photon: bool = True):
        params = self.to_dict(name=name,
                              storage=storage,
                              target=target,
                              continuous=continuous,
                              development=development,
                              configuration=configuration,
                              notebooks=notebooks,
                              libraries=libraries,
                              clusters=clusters,
                              min_workers=min_workers,
                              max_workers=max_workers,
                              photon=photon)
        return self.create_from_dict(params)

    @staticmethod
    def to_dict(name: str, storage: str, target: str, continuous: bool = False, development: bool = True, configuration: dict = None, notebooks: list = None, libraries: list = None, clusters: list = None, min_workers: int = 0, max_workers: int = 0, photon: bool = True):
        
        if configuration is None:
            configuration = {}
        assert type(configuration) == dict, f"Expected configuration to be of type dict, found {type(configuration)}"

        if clusters is None:
            clusters = []
        assert type(clusters) == list, f"Expected clusters to be of type list, found {type(clusters)}"

        if len(clusters) == 0:
            if min_workers == 0 and max_workers == 0:
                configuration["spark.master"] = "local[*]"

            if min_workers == max_workers:
                clusters.append({
                    "label": "default",
                    "num_workers": min_workers,
                })
            else:
                clusters.append({
                    "label": "default",
                    "autoscale": {
                        "min_workers": min_workers,
                        "max_workers": max_workers,
                    }
                })

        if notebooks is not None:
            libraries = []
            for notebook in notebooks:
                libraries.append({
                    "notebook": {
                        "path": notebook
                    }    
                })

        if libraries is not None:
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
        params["photon"] = photon

        return params

    def start_by_id(self, pipeline_id: str):
        return self.client.execute_post_json(f"{self.base_uri}/{pipeline_id}/updates", dict())

    def start_by_name(self, name: str):
        pipeline_id = self.get_by_name(name).get("pipeline_id")
        return self.start_by_id(pipeline_id)
