from enum import Enum


class Cloud(Enum):
    AWS = "AWS"
    MSA = "MSA"
    GCP = "GCP"
    UNKNOWN = "UNKNOWN"

    @property
    def is_aws(self) -> bool:
        return self == Cloud.AWS

    @property
    def is_msa(self) -> bool:
        return self == Cloud.MSA

    @property
    def is_gcp(self) -> bool:
        return self == Cloud.GCP

    # @staticmethod
    # def is_aws(actual_cloud: Union[str, "Cloud"]) -> bool:
    #     """
    #     :param actual_cloud: the actual value against which to test or `Cloud.current` if `actual_cloud == None`
    #     :return: True if this is a workspace backed by Amazon Web Services (AWS)
    #     """
    #     actual_cloud = actual_cloud if type(actual_cloud) == str else actual_cloud.value
    #     return Cloud.AWS.value == actual_cloud
    #
    # @staticmethod
    # def is_msa(actual_cloud: Union[str, "Cloud"]) -> bool:
    #     """
    #     :param actual_cloud: the actual value against which to test or `Cloud.current` if `actual_cloud == None`
    #     :return: True if this is a workspace backed by Microsoft Azure (MSA)
    #     """
    #     actual_cloud = actual_cloud if type(actual_cloud) == str else actual_cloud.value
    #     return Cloud.MSA.value == actual_cloud
    #
    # @staticmethod
    # def is_gcp(actual_cloud: Union[str, "Cloud"]) -> bool:
    #     """
    #     :param actual_cloud: the actual value against which to test or `Cloud.current` if `actual_cloud == None`
    #     :return: True if this is a workspace backed by Google Cloud Platform (GCP)
    #     """
    #     actual_cloud = actual_cloud if type(actual_cloud) == str else actual_cloud.value
    #     return Cloud.GCP.value == actual_cloud

    @staticmethod
    def current_cloud() -> "Cloud":
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
