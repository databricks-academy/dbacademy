__all__ = ["from_publisher", "from_translator"]

from typing import Optional
from dbacademy.common import validate
from dbacademy.dbbuild.build_config_data import BuildConfigData
from dbacademy.dbbuild.publish.publisher import Publisher
from dbacademy.dbbuild.publish.translator import Translator
from dbacademy.dbbuild.publish.publishing_info import Translation


class ArtifactValidator:

    def __init__(self, *,
                 build_config: BuildConfigData,
                 target_repo_url: str,
                 temp_repo_dir: str,
                 temp_work_dir: str,
                 translation: Optional[Translation],
                 common_language: str) -> None:

        self.__build_config = validate(build_config=build_config).required.as_type(BuildConfigData)
        self.__translation: Translation = validate(translation=translation).required.as_type(Translation)

        self.__target_repo_url = validate(target_repo_url=target_repo_url).required.str()
        self.__temp_repo_dir = validate(temp_repo_dir=temp_repo_dir).required.str()
        self.__temp_work_dir = validate(temp_work_dir=temp_work_dir).required.str()
        self.__common_language = validate(common_language=common_language).required.str()

    @property
    def translation(self) -> Optional[Translation]:
        return self.__translation

    @property
    def target_repo_url(self) -> str:
        return self.__target_repo_url

    @property
    def temp_repo_dir(self) -> str:
        return self.__temp_repo_dir

    @property
    def temp_work_dir(self) -> str:
        return self.__temp_work_dir

    @property
    def common_language(self) -> str:
        return self.__common_language

    @property
    def build_config(self) -> BuildConfigData:
        return self.__build_config

    def validate_publishing_processes(self) -> None:
        from dbacademy.dbhelper.validations import ValidationSuite

        suite = ValidationSuite(name="Distribution")
        suite.test_true(actual_value=lambda: self.__validate_distribution_dbc(), description=f"DBC in Distribution System (v{self.build_config.version}-PENDING)", depends_on=[])

        if self.target_repo_url is not None:
            suite.test_true(actual_value=lambda: self.__validate_git_releases_dbc(), description=f"Found \"{self.build_config.version}\" in Version Info in DBC from GitHub", depends_on=[])
            suite.test_true(actual_value=lambda: self.__validate_git_branch(branch="published", version=None), description=f"Found \"{self.build_config.version}\" in Version Info from GitHub Repo (published)", depends_on=[])
            suite.test_true(actual_value=lambda: self.__validate_git_branch(branch=f"published-v{self.build_config.version}", version=None), description=f"Found \"{self.build_config.version}\" in Version Info from GitHub Repo (published-v{self.build_config.version})", depends_on=[])

        suite.test_true(actual_value=lambda: self.__validate_published_docs(version=self.build_config.version), description=f"Docs Published as PDF ({self.build_config.version})", depends_on=[])

        suite.display_results()
        assert suite.passed, f"One or more problems were found."

    def __validate_distribution_dbc(self) -> bool:
        from dbacademy import dbgems

        file_name = f"v{self.build_config.version}-PENDING/{self.build_config.build_name}.dbc"

        print()
        print(f"Validating the DBC in DBAcademy's distribution system ({self.build_config.version}):")

        target_path = f"dbfs:/mnt/resources.training.databricks.com/distributions/{self.build_config.build_name}/{file_name}"
        files = dbgems.dbutils.fs.ls(target_path)  # Generates an un-catchable exception
        assert len(files) == 1, f"The distribution DBC was not found at \"{target_path}\"."

        print(f"| PASSED:  .../{file_name} found in \"s3://resources.training.databricks.com/distributions/{self.build_config.build_name}/\".")
        print(f"| UNKNOWN: \"v{self.build_config.version}\" found in \"s3://resources.training.databricks.com/distributions/{self.build_config.build_name}/{file_name}\".")

        return True

    def __validate_git_releases_dbc(self, version: Optional[str] = None) -> bool:
        print()
        print("Validating the DBC in GitHub's Releases page:")

        version = version or self.build_config.version

        base_url = self.target_repo_url[:-4] if self.target_repo_url.endswith(".git") else self.target_repo_url
        dbc_url = f"{base_url}/releases/download/v{version}/{self.build_config.build_name}-v{self.build_config.version}-notebooks.dbc"

        return self.__validate_dbc(version=version, dbc_url=dbc_url)

    def __validate_dbc(self, version: Optional[str] = None, dbc_url: str = None) -> bool:
        from dbacademy import dbgems

        version = version or self.build_config.version

        dbc_target_dir = f"{self.temp_work_dir}/{self.build_config.build_name}-v{version}"[10:]

        name = dbc_url.split("/")[-1]
        print(f"| Importing: {name}")
        print(f"| Source:    {dbc_url}")
        print(f"| Target:    {dbc_target_dir}")
        print(f"| Notebooks: {dbgems.get_workspace_url()}#workspace{dbc_target_dir}")

        self.build_config.client.workspace.delete_path(dbc_target_dir, recursive=True)
        self.build_config.client.workspace.mkdirs(dbc_target_dir)
        self.build_config.client.workspace.import_dbc_files(path=dbc_target_dir, source_url=dbc_url, overwrite=True)

        return self.__validate_version_info(version=version, dbc_dir=dbc_target_dir)

    def __validate_version_info(self, *, version: Optional[str], dbc_dir: str) -> bool:
        version = version or self.build_config.version

        version_info_path = f"{dbc_dir}/Version Info"
        source = self.build_config.client.workspace.export_notebook(version_info_path)
        assert f"**{version}**" in source, f"Expected the notebook \"Version Info\" at \"{version_info_path}\" to contain the version \"{version}\""
        print(f"|")
        print(f"| PASSED: v{version} found in \"{version_info_path}\"")

        return True

    def __validate_git_branch(self, *, branch: str, version: Optional[str]) -> bool:
        from dbacademy import common
        from dbacademy.dbbuild.build_utils import BuildUtils

        print()
        print(f"Validating the \"{branch}\" branch in the public, student-facing repo:")

        if not self.build_config.i18n:
            repo_url = f"https://github.com/databricks-academy/{self.build_config.build_name}.git"
        else:
            repo_url = f"https://github.com/databricks-academy/{self.build_config.build_name}-{self.common_language}.git"

        name = common.clean_string(self.build_config.username)
        target_dir = f"{self.temp_repo_dir}/{name}-{self.build_config.build_name}-{branch}"
        BuildUtils.reset_git_repo(client=self.build_config.client,
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
            print(f"| PASSED: No documents to validate.")
            return True

        print()
        print(f"Validating export of Google docs ({version})")

        google_client = google.from_workspace()
        docs_publisher = DocsPublisher(build_name=self.build_config.build_name, version=self.build_config.version, translation=self.translation)

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


def from_publisher(publisher: Publisher) -> ArtifactValidator:
    from dbacademy.dbbuild.publish.publishing_info import PublishingInfo

    publisher = validate(publisher=publisher).required.as_type(Publisher)

    info = PublishingInfo(publisher.build_config.publishing_info)

    return ArtifactValidator(build_config=publisher.build_config,
                             target_repo_url=publisher.target_repo_url,
                             temp_repo_dir=publisher.temp_repo_dir,
                             temp_work_dir=publisher.temp_work_dir,
                             translation=info.translations.get("english"),
                             common_language=publisher.common_language)


def from_translator(translator: Translator) -> ArtifactValidator:
    from dbacademy.dbbuild.publish.publishing_info import PublishingInfo

    translator = validate(translator=translator).required.as_type(Translator)
    info = PublishingInfo(translator.build_config.publishing_info)

    return ArtifactValidator(build_config=translator.build_config,
                             target_repo_url=translator.target_repo_url,
                             temp_repo_dir=translator.temp_repo_dir,
                             temp_work_dir=translator.temp_work_dir,
                             translation=info.translations.get(translator.common_language),
                             common_language=translator.common_language)
