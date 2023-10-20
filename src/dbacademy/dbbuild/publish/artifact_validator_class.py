__all__ = ["ArtifactValidator"]

from typing import Optional
from dbacademy.dbbuild.publish.publisher_class import Publisher
from dbacademy.dbbuild.publish.translator_class import Translator
from dbacademy.dbbuild.publish.publishing_info_class import Translation
from dbacademy.clients.databricks import DBAcademyRestClient


class ArtifactValidator:

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
                                 i18n=publisher.i18n,
                                 common_language=publisher.common_language,)

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
                                 i18n=translator.i18n,
                                 common_language=translator.common_language)

    def __init__(self, *, build_name: str, version: str, core_version: str, client: DBAcademyRestClient, target_repo_url: str, temp_repo_dir: str, temp_work_dir: str, username: str, translation: Translation, i18n: bool, common_language: str) -> None:
        from dbacademy.common import validate
        from dbacademy.dbbuild.publish.publishing_info_class import Translation

        self.build_name = build_name
        self.version = version
        self.core_version = core_version
        self.client = client

        self.target_repo_url = target_repo_url
        self.temp_repo_dir = temp_repo_dir
        self.temp_work_dir = temp_work_dir
        self.username = username

        self.i18n = i18n
        self.common_language = common_language

        self.translation = None if translation is None else validate.any_value(Translation, translation=translation)

    def validate_publishing_processes(self) -> None:
        from dbacademy.dbhelper.validations.validation_suite_class import ValidationSuite

        suite = ValidationSuite(name="Distribution")
        suite.test_true(actual_value=lambda: self.__validate_distribution_dbc(), description=f"DBC in Distribution System (v{self.version}-PENDING)", depends_on=[])

        if self.target_repo_url is not None:
            suite.test_true(actual_value=lambda: self.__validate_git_releases_dbc(), description=f"Found \"{self.version}\" in Version Info in DBC from GitHub", depends_on=[])
            suite.test_true(actual_value=lambda: self.__validate_git_branch(branch="published", version=None), description=f"Found \"{self.version}\" in Version Info from GitHub Repo (published)", depends_on=[])
            suite.test_true(actual_value=lambda: self.__validate_git_branch(branch=f"published-v{self.version}", version=None), description=f"Found \"{self.version}\" in Version Info from GitHub Repo (published-v{self.version})", depends_on=[])

        suite.test_true(actual_value=lambda: self.__validate_published_docs(version=self.version), description=f"Docs Published as PDF ({self.version})", depends_on=[])

        suite.display_results()
        assert suite.passed, f"One or more problems were found."

    def __validate_distribution_dbc(self) -> True:
        from dbacademy import dbgems

        file_name = f"v{self.version}-PENDING/{self.build_name}.dbc"

        print()
        print(f"Validating the DBC in DBAcademy's distribution system ({self.version}):")

        target_path = f"dbfs:/mnt/resources.training.databricks.com/distributions/{self.build_name}/{file_name}"
        files = dbgems.dbutils.fs.ls(target_path)  # Generates an un-catchable exception
        assert len(files) == 1, f"The distribution DBC was not found at \"{target_path}\"."

        print(f"| PASSED:  .../{file_name} found in \"s3://resources.training.databricks.com/distributions/{self.build_name}/\".")
        print(f"| UNKNOWN: \"v{self.version}\" found in \"s3://resources.training.databricks.com/distributions/{self.build_name}/{file_name}\".")

        return True

    def __validate_git_releases_dbc(self, version=None) -> bool:
        print()
        print("Validating the DBC in GitHub's Releases page:")

        version = version or self.version

        base_url = self.target_repo_url[:-4] if self.target_repo_url.endswith(".git") else self.target_repo_url
        dbc_url = f"{base_url}/releases/download/v{version}/{self.build_name}-v{self.version}-notebooks.dbc"

        return self.__validate_dbc(version=version, dbc_url=dbc_url)

    def __validate_dbc(self, version=None, dbc_url=None) -> bool:
        from dbacademy import dbgems

        version = version or self.version

        dbc_target_dir = f"{self.temp_work_dir}/{self.build_name}-v{version}"[10:]

        name = dbc_url.split("/")[-1]
        print(f"| Importing: {name}")
        print(f"| Source:    {dbc_url}")
        print(f"| Target:    {dbc_target_dir}")
        print(f"| Notebooks: {dbgems.get_workspace_url()}#workspace{dbc_target_dir}")

        self.client.workspace.delete_path(dbc_target_dir)
        self.client.workspace.mkdirs(dbc_target_dir)
        self.client.workspace.import_dbc_files(dbc_target_dir, source_url=dbc_url)

        return self.__validate_version_info(version=version, dbc_dir=dbc_target_dir)

    def __validate_version_info(self, *, version: str, dbc_dir: str) -> bool:
        version = version or self.version

        version_info_path = f"{dbc_dir}/Version Info"
        source = self.client.workspace.export_notebook(version_info_path)
        assert f"**{version}**" in source, f"Expected the notebook \"Version Info\" at \"{version_info_path}\" to contain the version \"{version}\""
        print(f"|")
        print(f"| PASSED: v{version} found in \"{version_info_path}\"")

        return True

    def __validate_git_branch(self, *, branch: str, version: Optional[str]) -> bool:
        from dbacademy import common
        from dbacademy.dbbuild.build_utils_class import BuildUtils

        print()
        print(f"Validating the \"{branch}\" branch in the public, student-facing repo:")

        if not self.i18n:
            repo_url = f"https://github.com/databricks-academy/{self.build_name}.git"
        else:
            repo_url = f"https://github.com/databricks-academy/{self.build_name}-{self.common_language}.git"

        name = common.clean_string(self.username)
        target_dir = f"{self.temp_repo_dir}/{name}-{self.build_name}-{branch}"
        BuildUtils.reset_git_repo(client=self.client,
                                  directory=target_dir,
                                  repo_url=repo_url,
                                  branch=branch,
                                  which=None,
                                  prefix="| ")

        return self.__validate_version_info(version=version, dbc_dir=target_dir)

    def __validate_published_docs(self, version: str) -> bool:
        import os
        from dbacademy.dbbuild.publish.docs_publisher import DocsPublisher
        from dbacademy.clients import google

        if self.translation is None:
            print(f"| PASSED: No documents to validate")
            return True

        print()
        print(f"Validating export of Google docs ({version})")

        google_client = google.from_workspace()
        docs_publisher = DocsPublisher(build_name=self.build_name, version=self.version, translation=self.translation)

        total = len(self.translation.document_links)
        for i, link in enumerate(self.translation.document_links):
            file_id = google_client.drive.to_gdoc_id(gdoc_url=link)
            file = google_client.drive.file_get(file_id)
            name = file.get("name")
            folder_id = file.get("id")
            print(f"| {i+1} of {total}: {name} (https://drive.google.com/drive/folders/{folder_id})")

            distribution_path = docs_publisher.get_distribution_path(version=version, file=file)
            print(f"|                   {distribution_path}")

            assert os.path.exists(distribution_path), f"The document {name} was not found at \"{distribution_path}\""

        print(f"| PASSED: All documents exported to the distribution system")

        return True
