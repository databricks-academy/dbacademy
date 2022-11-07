from typing import Optional
from dbacademy import dbgems
from dbacademy import common


class ArtifactValidator:
    from dbacademy.dbbuild.publish.publisher_class import Publisher
    from dbacademy.dbbuild.publish.translator_class import Translator
    from dbacademy.dbrest import DBAcademyRestClient
    from dbacademy.dbbuild.publish.publishing_info_class import Translation

    @staticmethod
    def from_publisher(publisher: Publisher) -> "ArtifactValidator":
        from dbacademy.dbbuild.publish.publishing_info_class import PublishingInfo, Translation

        info = PublishingInfo(publisher.build_config.publishing_info)
        translation: Translation = info.translations.get("english")

        return ArtifactValidator(build_name=publisher.build_name,
                                 version=publisher.version,
                                 core_version=publisher.core_version,
                                 client=publisher.client,
                                 target_repo_url=publisher.target_repo_url,
                                 temp_repo_dir=publisher.temp_repo_dir,
                                 temp_work_dir=publisher.temp_work_dir,
                                 username=publisher.username,
                                 translation=translation,
                                 common_language=None,)

    @staticmethod
    def from_translator(translator: Translator) -> "ArtifactValidator":
        from dbacademy.dbbuild.publish.publishing_info_class import PublishingInfo, Translation

        info = PublishingInfo(translator.build_config.publishing_info)
        translation: Translation = info.translations.get(translator.common_language)

        return ArtifactValidator(build_name=translator.build_name,
                                 version=translator.version,
                                 core_version=translator.core_version,
                                 client=translator.client,
                                 target_repo_url=translator.target_repo_url,
                                 temp_repo_dir=translator.temp_repo_dir,
                                 temp_work_dir=translator.temp_work_dir,
                                 username=translator.username,
                                 translation=translation,
                                 common_language=translator.common_language)

    def __init__(self, *, build_name: str, version: str, core_version: str, client: DBAcademyRestClient, target_repo_url: str, temp_repo_dir: str, temp_work_dir: str, username: str, translation: Translation, common_language: Optional[str]) -> None:
        from dbacademy.dbbuild.publish.publishing_info_class import Translation

        self.build_name = build_name
        self.version = version
        self.core_version = core_version
        self.client = client

        self.target_repo_url = target_repo_url
        self.temp_repo_dir = temp_repo_dir
        self.temp_work_dir = temp_work_dir
        self.username = username
        self.common_language = common_language

        self.translation = common.validate_type(translation, "translation", Translation)

    def validate_publishing_processes(self) -> None:
        self.__validate_distribution_dbc(as_latest=True)
        print()
        print("-" * 80)
        print()

        self.__validate_distribution_dbc(as_latest=False)
        print()
        print("-" * 80)
        print()

        self.__validate_git_releases_dbc()
        print()
        print("-" * 80)
        print()

        self.__validate_git_branch(branch="published", version=None)
        print()
        print("-" * 80)
        print()

        self.__validate_git_branch(branch=f"published-v{self.version}", version=None)
        print()
        print("-" * 80)
        print()

        self.__validate_published_docs()
        print()
        print("-" * 80)
        print()

    def __validate_distribution_dbc(self, as_latest: bool) -> None:

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

        print(f"PASSED:  .../{file_name} found in \"s3://secured.training.databricks.com/distributions/{self.build_name}/\".")
        print(f"UNKNOWN: \"v{self.version}\" found in \"s3://secured.training.databricks.com/distributions/{self.build_name}/{file_name}\".")

    def __validate_git_releases_dbc(self, version=None) -> None:
        print("Validating the DBC in GitHub's Releases page\n")

        version = version or self.version
        # core_version = version.split("-")[0]

        base_url = self.target_repo_url[:-4] if self.target_repo_url.endswith(".git") else self.target_repo_url
        dbc_url = f"{base_url}/releases/download/v{version}/{self.build_name}-v{self.version}-notebooks.dbc"

        return self.__validate_dbc(version=version,
                                   dbc_url=dbc_url)

    def __validate_dbc(self, version=None, dbc_url=None) -> None:
        version = version or self.version

        dbc_target_dir = f"{self.temp_work_dir}/{self.build_name}-v{version}"[10:]

        name = dbc_url.split("/")[-1]
        print(f"Importing {name}")
        print(f"| Source:    {dbc_url}")
        print(f"| Target:    {dbc_target_dir}")
        print(f"| Notebooks: {dbgems.get_workspace_url()}#workspace{dbc_target_dir}")

        self.client.workspace.delete_path(dbc_target_dir)
        self.client.workspace.mkdirs(dbc_target_dir)
        self.client.workspace.import_dbc_files(dbc_target_dir, source_url=dbc_url)

        print()
        self.__validate_version_info(version=version, dbc_dir=dbc_target_dir)

    def __validate_version_info(self, *, version: str, dbc_dir: str) -> None:
        version = version or self.version

        version_info_path = f"{dbc_dir}/Version Info"
        source = self.client.workspace.export_notebook(version_info_path)
        assert f"**{version}**" in source, f"Expected the notebook \"Version Info\" at \"{version_info_path}\" to contain the version \"{version}\""
        print(f"PASSED: v{version} found in \"{version_info_path}\"")

    def __validate_git_branch(self, *, branch: str, version: Optional[str]) -> None:
        from ..build_utils_class import BuildUtils

        print(f"Validating the \"{branch}\" branch in the public, student-facing repo.\n")

        if self.common_language is None:
            repo_url = f"https://github.com/databricks-academy/{self.build_name}.git"
        else:
            repo_url = f"https://github.com/databricks-academy/{self.build_name}-{self.common_language}.git"

        target_dir = f"{self.temp_repo_dir}/{self.username}-{self.build_name}-{branch}"
        BuildUtils.reset_git_repo(client=self.client,
                                  directory=target_dir,
                                  repo_url=repo_url,
                                  branch=branch,
                                  which=None)

        self.__validate_version_info(version=version, dbc_dir=target_dir)

    def __validate_published_docs(self) -> None:
        import os
        from dbacademy.dbbuild.publish.docs_publisher import DocsPublisher

        docs_publisher = DocsPublisher(build_name=self.build_name, version=self.version, translation=self.translation)

        for link in self.translation.document_links:
            file = docs_publisher.get_file(gdoc_url=link)
            name = file.get("name")

            for version in [self.version, "LATEST"]:
                distribution_path = docs_publisher.get_distribution_path(version=version, file=file)
                assert os.path.exists(distribution_path), f"The document {name} was not found at \"{distribution_path}\""
