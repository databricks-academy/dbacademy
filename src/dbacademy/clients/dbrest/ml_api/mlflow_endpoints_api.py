__all__ = ["MLflowEndpointsApi"]

from typing import Dict, Any, List, Optional
from dbacademy.clients.rest.common import ApiClient, ApiContainer


class MLflowEndpointsApi(ApiContainer):

    def __init__(self, client: ApiClient):
        from dbacademy.common import validate

        self.__client = validate(client=client).required.as_type(ApiClient)
        self.base_uri = f"{self.__client.endpoint}/api/2.0/preview/mlflow/endpoints"

    def list(self) -> List[Dict[str, Any]]:
        response = self.__client.api("GET", f"{self.base_uri}/list")
        return response.get("endpoints", list())

    def get_status(self, model_name: str) -> Dict[str, Any]:
        response = self.__client.api("GET", f"{self.base_uri}/get-status?registered_model_name={model_name}")
        return response.get("endpoint_status")

    def enable(self, model_name: str) -> None:
        payload = {
            "registered_model_name": model_name
        }
        self.__client.api("POST", f"{self.base_uri}/enable", _data=payload)

    def disable(self, model_name: str) -> None:
        payload = {
            "registered_model_name": model_name
        }
        self.__client.api("POST", f"{self.base_uri}/disable", _data=payload)

    def wait_for_endpoint(self, model_name: str, expected_state: str = "ENDPOINT_STATE_READY", delay_seconds: int = 10, timeout: int = 10*60) -> None:
        import time

        start = int(time.time())

        while int(time.time())-start < timeout:
            endpoint_status = self.get_status(model_name)
            state = endpoint_status.get("state")

            if state == expected_state:
                # Give it a couple extra seconds to complete the transition
                time.sleep(delay_seconds)
                return print(f"Endpoint is {state}")

            print(f"The endpoint is not {expected_state} ({state}), waiting {delay_seconds} seconds")
            time.sleep(delay_seconds)  # Wait N seconds

        raise Exception(f"Wait process timed out after {timeout} seconds")

    def list_endpoint_versions(self, model_name: Optional[str]) -> List[Dict[str, Any]]:
        url = f"{self.base_uri}/list-versions"
        if model_name is not None:
            url += f"?registered_model_name={model_name}"

        response = self.__client.api("GET", url)
        return response.get("endpoint_versions", list())

    def wait_for_endpoint_version(self, model_name: str, version_name: str, delay_seconds: int = 10, timeout: int = 10*60) -> None:
        import time

        start = int(time.time())

        while int(time.time()) - start < timeout:
            for version in self.list_endpoint_versions(model_name):
                if version.get("endpoint_version_name") == str(version_name):
                    state = version.get("state")
                    if state == "VERSION_STATE_READY":
                        time.sleep(delay_seconds)  # Give it a couple extra seconds to complete the transition
                        return print(f"Endpoint version is ready ({state})")
                    else:
                        print(f"Version not ready ({state}), waiting {delay_seconds} seconds")
                        time.sleep(delay_seconds)

        raise Exception(f"Wait process timed out after {timeout} seconds")
