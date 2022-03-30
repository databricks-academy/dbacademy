# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient


class PipelinesClient:
    def __init__(self, client: DBAcademyRestClient, token: str, endpoint: str):
        self.client = client
        self.token = token
        self.endpoint = endpoint

        self.base_uri = f"{self.endpoint}/api/2.0/pipelines"

    def list(self):
        return self.client.execute_get_json(f"{self.base_uri}")

    # def list_events_by_id(self):
    #     return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}/events")

    # def list_events_by_id(self):
    #     return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}/events")

    def get_by_id(self, pipeline_id):
        return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}")

    # def get_updates_by_id(self, pipeline_id, update_id):
    #     return self.client.execute_get_json(f"{self.base_uri}/{pipeline_id}/updates/{update_id}")

    def delete_by_id(self, pipelines):
        return self.client.execute_delete_json(f"{self.base_uri}/{pipeline_id}")

    # def create_from_dict(self, params:dict):
    #     return self.client.execute_post_json(f"{self.base_uri}", params)

    # def create(self, name:str, query:str, description:str=None, schedule:dict=None, options:dict=None, data_source_id=None):
    #     params = dict()
    #     params["name"] = x
    #     params["storage"] = x
    #     params["clusters"] = x
    #     params["libraries"] = x
    #     params["continuous"] = x

    #     # {
    #     #     "statuses": [
    #     #         {
    #     #             "pipeline_id": "01f0510d-f1b5-4dbe-bdf3-65f742ed5925",
    #     #             "state": "IDLE",
    #     #             "name": "djs-etl-dlt-python",
    #     #             "creator_user_name": "douglas.strodtman@databricks.com",
    #     #             "run_as_user_name": "douglas.strodtman@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "1411fccb-f10f-4fcd-b01f-ff00983bf538",
    #     #             "state": "IDLE",
    #     #             "name": "Jobs-Lab-92-jacob.parr@databricks.com",
    #     #             "latest_updates": [
    #     #                 {
    #     #                     "update_id": "119e0aba-c5eb-4545-a42b-b8cb45ed1152",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T22:46:26.871Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "937d9415-90e7-4ef4-9d76-3abf92a2e056",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T22:40:26.719Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "a4c630aa-a2d8-4dc0-a2d9-8ee782850eff",
    #     #                     "state": "CANCELED",
    #     #                     "creation_time": "2022-03-14T22:38:04.188Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "a11de913-da50-4df8-a0c8-50f2cc852a93",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T22:30:59.273Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "ca5fe276-bb30-4051-bf0e-49248d4aefe0",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T22:26:09.235Z"
    #     #                 }
    #     #             ],
    #     #             "creator_user_name": "jacob.parr@databricks.com",
    #     #             "run_as_user_name": "jacob.parr@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "17c84bb2-fa71-4b8f-b9d6-60d97d1b22d4",
    #     #             "state": "IDLE",
    #     #             "name": "djs-etl-dlt-sql",
    #     #             "latest_updates": [
    #     #                 {
    #     #                     "update_id": "f0e406b8-a251-4581-81e8-dfff0a9f483b",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-22T20:25:25.439Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "c0f35152-d72c-484c-9718-6ff30a575062",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-17T14:10:44.904Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "7ca72730-7ae5-40d2-8ba1-341e09ae0879",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-11T19:48:06.414Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "2e182e46-3852-40c0-835a-55d0e16c48e7",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-11T19:45:34.857Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "7ce9d316-2a82-474a-a92f-2a6e28fd9b2b",
    #     #                     "state": "FAILED",
    #     #                     "creation_time": "2022-03-11T19:44:53.623Z"
    #     #                 }
    #     #             ],
    #     #             "creator_user_name": "douglas.strodtman@databricks.com",
    #     #             "run_as_user_name": "douglas.strodtman@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "3be3b2ed-472e-4fd6-9b67-4a7dd7bb08ed",
    #     #             "state": "IDLE",
    #     #             "name": "jl-dlt-test",
    #     #             "latest_updates": [
    #     #                 {
    #     #                     "update_id": "254fe515-2b25-425f-afaa-8a2f61b59381",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-11T17:32:58.289Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "dffcaa74-8e90-44a1-8712-ee614292dab3",
    #     #                     "state": "CANCELED",
    #     #                     "creation_time": "2022-03-11T17:32:57.294Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "d4c97d95-c18c-4151-95fc-479fcb2e4444",
    #     #                     "state": "CANCELED",
    #     #                     "creation_time": "2022-03-11T17:32:40.723Z"
    #     #                 }
    #     #             ],
    #     #             "creator_user_name": "juno.lee@databricks.com",
    #     #             "run_as_user_name": "juno.lee@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "46338807-3bcc-4cdd-ae04-f8c81db4ef5d",
    #     #             "state": "IDLE",
    #     #             "name": "DLT-Demo-81-jacob.parr@databricks.com",
    #     #             "latest_updates": [
    #     #                 {
    #     #                     "update_id": "76b765bb-1e0d-43ad-8611-fafd09eb541e",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-26T17:10:43.499Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "e94940e0-3a50-4488-a331-87578e29f781",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T21:15:22.573Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "4469ee5a-ffca-40bd-a58e-53535422f27e",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T21:11:06.635Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "54752ef9-bbbe-4b02-aed4-1cb5e25fae00",
    #     #                     "state": "FAILED",
    #     #                     "creation_time": "2022-03-14T21:05:15.749Z"
    #     #                 }
    #     #             ],
    #     #             "creator_user_name": "jacob.parr@databricks.com",
    #     #             "run_as_user_name": "jacob.parr@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "a5e193e2-4017-4412-b21c-594951edbb94",
    #     #             "state": "IDLE",
    #     #             "name": "djs-dlt-pipeline",
    #     #             "creator_user_name": "douglas.strodtman@databricks.com",
    #     #             "run_as_user_name": "douglas.strodtman@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "b01d801a-e839-40f4-a137-2ffcad959b14",
    #     #             "state": "IDLE",
    #     #             "name": "djs-new-test",
    #     #             "creator_user_name": "douglas.strodtman@databricks.com",
    #     #             "run_as_user_name": "douglas.strodtman@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "d7bfddbf-27ff-4529-a77d-28f21d90b1bf",
    #     #             "state": "IDLE",
    #     #             "name": "djs-dlt-demo",
    #     #             "creator_user_name": "douglas.strodtman@databricks.com",
    #     #             "run_as_user_name": "douglas.strodtman@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "d8c07837-ec84-4f1e-8799-7aa54e4fdc4f",
    #     #             "state": "IDLE",
    #     #             "name": "Cap-12-jacob.parr@databricks.com",
    #     #             "latest_updates": [
    #     #                 {
    #     #                     "update_id": "7cb27873-ebfb-42e8-8e76-aac96afd3f3d",
    #     #                     "state": "CANCELED",
    #     #                     "creation_time": "2022-03-16T20:16:34.563Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "08e83464-9ae8-4de2-b249-7d64354c62cc",
    #     #                     "state": "CANCELED",
    #     #                     "creation_time": "2022-03-15T15:58:41.211Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "772339de-6e74-4954-835c-fadbe8c9939e",
    #     #                     "state": "CANCELED",
    #     #                     "creation_time": "2022-03-15T15:53:20.458Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "243bbddc-b079-4ef1-b014-fda1901b246d",
    #     #                     "state": "FAILED",
    #     #                     "creation_time": "2022-03-15T15:33:37.344Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "8d869bfa-997b-41d3-8e7a-bc72aba7e6f4",
    #     #                     "state": "CANCELED",
    #     #                     "creation_time": "2022-03-15T15:32:17.998Z"
    #     #                 }
    #     #             ],
    #     #             "creator_user_name": "jacob.parr@databricks.com",
    #     #             "run_as_user_name": "jacob.parr@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "e9c2539d-fb92-482f-a167-cab7e556d4b7",
    #     #             "state": "IDLE",
    #     #             "name": "Jobs-Demo-91-jacob.parr@databricks.com",
    #     #             "latest_updates": [
    #     #                 {
    #     #                     "update_id": "000b0cbe-1bda-44c1-ac21-b412f7ce924e",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T22:59:43.390Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "8fbf4fb1-3812-4ea9-a4e4-110fa531f24e",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T22:57:10.405Z"
    #     #                 },
    #     #                 {
    #     #                     "update_id": "74c0bd9d-5336-42bf-a325-75cc554349d4",
    #     #                     "state": "COMPLETED",
    #     #                     "creation_time": "2022-03-14T21:31:03.988Z"
    #     #                 }
    #     #             ],
    #     #             "creator_user_name": "jacob.parr@databricks.com",
    #     #             "run_as_user_name": "jacob.parr@databricks.com"
    #     #         },
    #     #         {
    #     #             "pipeline_id": "f7c77b52-8a24-45cf-973c-77df1f2a880f",
    #     #             "state": "IDLE",
    #     #             "name": "djs-test-solution",
    #     #             "creator_user_name": "douglas.strodtman@databricks.com",
    #     #             "run_as_user_name": "douglas.strodtman@databricks.com"
    #     #         }
    #     #     ]
    #     # }
    #     return self.create_from_dict(params)
