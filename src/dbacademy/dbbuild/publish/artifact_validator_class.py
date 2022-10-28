from typing import Union
from dbacademy import dbgems, common


class ArtifactValidator:
    from .publisher_class import Publisher
    from .translator_class import Translator
    from dbacademy.dbrest import DBAcademyRestClient

    @staticmethod
    def from_publisher(publisher: Publisher):
        return ArtifactValidator(build_name=publisher.build_name,
                                 version=publisher.version,
                                 core_version=publisher.core_version,
                                 client=publisher.client,
                                 target_repo_url=publisher.target_repo_url,
                                 temp_repo_dir=publisher.temp_repo_dir,
                                 temp_work_dir=publisher.temp_work_dir,
                                 username=publisher.username)

    @staticmethod
    def from_translator(translator: Translator):
        return ArtifactValidator(build_name=translator.build_name,
                                 version=translator.version,
                                 core_version=translator.core_version,
                                 client=translator.client,
                                 target_repo_url=translator.target_repo_url,
                                 temp_repo_dir=translator.temp_repo_dir,
                                 temp_work_dir=translator.temp_work_dir,
                                 username=translator.username)

    def __init__(self, *, build_name: str, version: str, core_version: str, client: DBAcademyRestClient, target_repo_url: str, temp_repo_dir: str, temp_work_dir: str, username: str):
        self.build_name = build_name
        self.version = version
        self.core_version = core_version
        self.client = client

        self.target_repo_url = target_repo_url
        self.temp_repo_dir = temp_repo_dir
        self.temp_work_dir = temp_work_dir
        self.username = username

    def validate_publishing_processes(self):
        self.__validate_distribution_dbc(as_latest=True)
        print("-" * 80)
        self.__validate_distribution_dbc(as_latest=False)
        print("-" * 80)
        self.__validate_git_releases_dbc()
        print("-" * 80)
        self.__validate_git_branch(branch="published", version=None)
        print("-" * 80)
        self.__validate_git_branch(branch=f"published-v{self.core_version}", version=None)

    @common.deprecated(reason="Validator.validate_distribution_dbc() was deprecated, see Validator.validate_publishing_processes() instead")
    def validate_distribution_dbc(self, as_latest: bool):
        return self.__validate_distribution_dbc(as_latest)

    def __validate_distribution_dbc(self, as_latest: bool):

        if not as_latest:
            label = self.version
        else:
            label = "vLATEST"
            if "-" in self.version:
                pos = self.version.find("-")
                tail = self.version[pos:]
                label += tail

        file_name = f"{label}/notebooks.dbc" if as_latest else f"v{self.version}/{self.build_name}-v{self.version}-notebooks.dbc"

        print(f"Validating the DBC in DBAcademy's distribution system ({label})\n")

        target_path = f"dbfs:/mnt/secured.training.databricks.com/distributions/{self.build_name}/{file_name}"
        files = dbgems.dbutils.fs.ls(target_path)  # Generates an un-catchable exception
        assert len(files) == 1, f"The distribution DBC was not found at \"{target_path}\"."

        print(f"PASSED:  v{file_name} found in \"s3://secured.training.databricks.com/distributions/{self.build_name}/\".")
        print(f"UNKNOWN: v{self.version} found in \"s3://secured.training.databricks.com/distributions/{self.build_name}/{file_name}\".")

    @common.deprecated(reason="Validator.validate_distribution_dbc() was deprecated, see Validator.validate_publishing_processes() instead")
    def validate_git_releases_dbc(self, version=None):
        return self.__validate_git_releases_dbc(version)

    def __validate_git_releases_dbc(self, version=None):
        print("Validating the DBC in GitHub's Releases page\n")

        version = version or self.version
        core_version = version.split("-")[0]

        base_url = self.target_repo_url[:-4] if self.target_repo_url.endswith(".git") else self.target_repo_url
        dbc_url = f"{base_url}/releases/download/v{core_version}/{self.build_name}-v{self.version}-notebooks.dbc"

        return self.__validate_dbc(version=version,
                                   dbc_url=dbc_url)

    def __validate_dbc(self, version=None, dbc_url=None):
        version = version or self.version

        self.client.workspace.mkdirs(self.temp_work_dir)
        dbc_target_dir = f"{self.temp_work_dir}/{self.build_name}-v{version}"

        name = dbc_url.split("/")[-1]
        print(f"Importing {name}")
        print(f" - Source: {dbc_url}")
        print(f" - Target: {dbc_target_dir}")

        self.client.workspace.delete_path(dbc_target_dir)
        self.client.workspace.mkdirs(dbc_target_dir)
        self.client.workspace.import_dbc_files(dbc_target_dir, source_url=dbc_url)

        print()
        self.__validate_version_info(version=version, dbc_dir=dbc_target_dir)

    def __validate_version_info(self, *, version: str, dbc_dir: str):
        version = version or self.version

        version_info_path = f"{dbc_dir}/Version Info"
        source = self.client.workspace.export_notebook(version_info_path)
        assert f"**{version}**" in source, f"Expected the notebook \"Version Info\" at \"{version_info_path}\" to contain the version \"{version}\""
        print(f"PASSED: v{version} found in \"{version_info_path}\"")

    @common.deprecated(reason="Validator.validate_git_branch() was deprecated, see Validator.validate_publishing_processes() instead")
    def validate_git_branch(self, branch="published", version=None):
        self.__validate_git_branch(branch=branch, version=version)

    def __validate_git_branch(self, *, branch: str, version: Union[str, None]):
        from ..build_utils_class import BuildUtils

        print(f"Validating the \"{branch}\" branch in the public, student-facing repo.\n")

        target_dir = f"{self.temp_repo_dir}/{self.username}-{self.build_name}-{branch}"
        BuildUtils.reset_git_repo(client=self.client,
                                  directory=target_dir,
                                  repo_url=f"https://github.com/databricks-academy/{self.build_name}.git",
                                  branch=branch,
                                  which=None)
        print()
        self.__validate_version_info(version=version, dbc_dir=target_dir)
