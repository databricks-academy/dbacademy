from typing import Union
from enum import Enum


class Cloud(Enum):
    AWS = "AWS"
    MSA = "MSA"
    GCP = "GCP"
    UNKNOWN = "UNKNOWN"

    def is_aws(self, actual_cloud: Union[str, "Cloud"] = None) -> bool:
        """
        :param actual_cloud: the actual value against which to test or `Cloud.current` if `actual_cloud == None`
        :return: True if this is a workspace backed by Amazon Web Services (AWS)
        """
        actual_cloud = actual_cloud or self.current
        actual_cloud = actual_cloud if type(actual_cloud) == str else actual_cloud.value
        return self.value == actual_cloud

    def is_msa(self, actual_cloud: Union[str, "Cloud"] = None) -> bool:
        """
        :param actual_cloud: the actual value against which to test or `Cloud.current` if `actual_cloud == None`
        :return: True if this is a workspace backed by Microsoft Azure (MSA)
        """
        actual_cloud = actual_cloud or self.current
        actual_cloud = actual_cloud if type(actual_cloud) == str else actual_cloud.value
        return self.MSA.value == actual_cloud

    def is_gcp(self, actual_cloud: Union[str, "Cloud"] = None) -> bool:
        """
        :param actual_cloud: the actual value against which to test or `Cloud.current` if `actual_cloud == None`
        :return: True if this is a workspace backed by Google Cloud Platform (GCP)
        """
        actual_cloud = actual_cloud or self.current
        actual_cloud = actual_cloud if type(actual_cloud) == str else actual_cloud.value
        return self.GCP.value == actual_cloud

    @staticmethod
    def current() -> "Cloud":
        """
        Indicates which cloud the current workspace is deployed into
        :return: One of GCP, AWS, MSA or UNKNOWN if the current cloud cannot be auto-determined
        """
        import os

        config_path = "/databricks/common/conf/deploy.conf"
        if not os.path.exists(config_path):
            return Cloud.UNKNOWN

        with open(config_path) as f:
            for line in f:
                if "databricks.instance.metadata.cloudProvider" in line and "\"GCP\"" in line:
                    return Cloud.GCP
                elif "databricks.instance.metadata.cloudProvider" in line and "\"AWS\"" in line:
                    return Cloud.AWS
                elif "databricks.instance.metadata.cloudProvider" in line and "\"Azure\"" in line:
                    return Cloud.MSA

        return Cloud.UNKNOWN
