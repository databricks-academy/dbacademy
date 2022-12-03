from typing import Optional, List
from dbacademy import dbgems


class DatasetManager:

    @staticmethod
    def from_dbacademy_helper(da):
        return DatasetManager(data_source_uri=da.data_source_uri,
                              staging_source_uri=da.staging_source_uri,
                              datasets_path=da.paths.datasets,
                              remote_files=da.course_config.remote_files)

    def __init__(self, *, data_source_uri: str, staging_source_uri: str, datasets_path: str, remote_files: List[str]):
        """
        Creates an instance of DatasetManager
        :param data_source_uri: See DBAcademy.data_source_uri
        :param staging_source_uri: See DBAcademy.staging_source_uri
        :param datasets_path: See DBAcademy.paths.datasets, Paths
        """
        self.__fixes = 0
        self.__repaired_paths = []
        self.__data_source_uri = data_source_uri
        self.__staging_source_uri = staging_source_uri
        self.__datasets_path = datasets_path
        self.__remote_files = remote_files

    @property
    def remote_files(self) -> List[str]:
        return self.__remote_files

    @property
    def datasets_path(self) -> str:
        return self.__datasets_path

    @property
    def data_source_uri(self) -> str:
        return self.__data_source_uri

    @property
    def staging_source_uri(self):
        return self.__staging_source_uri

    @property
    def fixes(self) -> int:
        return self.__fixes

    @property
    def repaired_paths(self) -> List[str]:
        return self.__repaired_paths

    def install_dataset(self, *, install_min_time: str, install_max_time: str, reinstall_datasets: bool = False) -> None:
        """
        Install the datasets used by this course to DBFS.

        This ensures that data and compute are in the same region which subsequently mitigates performance issues
        when the storage and compute are, for example, on opposite sides of the world.
        """
        from dbacademy.dbhelper.paths_class import Paths
        # if not repairing_dataset: print(f"\nThe source for the datasets is\n{self.data_source_uri}/")
        # if not repairing_dataset: print(f"\nYour local dataset directory is {datasets_path}")

        if Paths.exists(self.datasets_path):
            # It's already installed...
            if reinstall_datasets:
                print(f"\nRemoving previously installed datasets")
                dbgems.dbutils.fs.rm(self.datasets_path, True)

            if not reinstall_datasets:
                print(f"\nSkipping install of existing datasets to \"{self.datasets_path}\"")
                self.validate_datasets(fail_fast=False)
                return

        print(f"\nInstalling datasets:")
        print(f"| from \"{self.data_source_uri}\"")
        print(f"| to \"{self.datasets_path}\"")
        if install_min_time is not None and install_max_time is not None:
            print(f"|")
            print(f"| NOTE: The datasets that we are installing are located in Washington, USA - depending on the")
            print(f"|       region that your workspace is in, this operation can take as little as {install_min_time} and")
            print(f"|       upwards to {install_max_time}, but this is a one-time operation.")

        # Using data_source_uri is a temporary hack because it assumes we can actually
        # reach the remote repository - in cases where it's blocked, this will fail.
        files = dbgems.dbutils.fs.ls(self.data_source_uri)

        total = len(files)
        what = "dataset" if total == 1 else "datasets"

        install_start = dbgems.clock_start()
        for i, f in enumerate(files):
            start = dbgems.clock_start()
            print(f"| copying {i+1}/{total} /{f.name[:-1]}", end="...")

            source_path = f"{self.data_source_uri}/{f.name}"
            target_path = f"{self.datasets_path}/{f.name}"

            dbgems.dbutils.fs.cp(source_path, target_path, True)
            print(dbgems.clock_stopped(start))

        self.validate_datasets(fail_fast=False)

        print(f"""\nThe install of the datasets completed successfully {dbgems.clock_stopped(install_start)}""")

    def validate_datasets(self, fail_fast: bool) -> None:
        """
        Validates the "install" of the datasets by recursively listing all files in the remote data repository as well as the local data repository, validating that each file exists but DOES NOT validate file size or checksum.
        """
        validation_start = dbgems.clock_start()

        if self.staging_source_uri == self.data_source_uri:
            # When working with staging data, we need to enumerate what is in there
            # and use it as a definitive source to the complete enumeration of our files
            start = dbgems.clock_start()
            print("\nEnumerating staged files for validation", end="...")
            self.__remote_files = DatasetManager.list_r(self.staging_source_uri)
            print(dbgems.clock_stopped(start))
            print()

        print(f"\nValidating the locally installed datasets:")

        self.__validate_and_repair()

        if self.fixes == 1:
            print(f"| fixed 1 issue", end="...")
        elif self.fixes > 0:
            print(f"| fixed {self.fixes} issues", end="...")
        else:
            print(f"| validation completed", end="...")

        print(dbgems.clock_stopped(validation_start, " total"))

        if fail_fast:
            assert self.fixes == 0, f"Unexpected modifications to source datasets."

    def __validate_and_repair(self) -> None:
        print("| listing local files", end="...")
        start = dbgems.clock_start()
        local_files = DatasetManager.list_r(self.datasets_path)
        print(dbgems.clock_stopped(start))

        # Process directories first
        self.__del_extra_paths(local_files)
        self.__add_extra_paths(local_files)

        # Then process individual files
        self.__del_extra_files(local_files)
        self.__add_extra_files(local_files)

    def __dataset_not_fixed(self, test_file: str) -> bool:
        for repaired_path in self.repaired_paths:
            if test_file.startswith(repaired_path):
                return False
        return True

    def __del_extra_paths(self, local_files: List[str]) -> None:
        """
        Removes extra directories (cascade effect vs one file at a time)
        :return: None
        """

        for file in local_files:
            if file not in self.remote_files and file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                self.repaired_paths.append(file)
                print(f"| removing extra path: {file}", end="...")
                dbgems.dbutils.fs.rm(f"{self.datasets_path}/{file[1:]}", True)
                print(dbgems.clock_stopped(start))

    def __add_extra_paths(self, local_files: List[str]) -> None:
        """
        Adds extra directories (cascade effect vs one file at a time)
        :return: None
        """
        for file in self.remote_files:
            if file not in local_files and file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                self.repaired_paths.append(file)
                print(f"| restoring missing path: {file}", end="...")
                source_file = f"{self.data_source_uri}/{file[1:]}"
                target_file = f"{self.datasets_path}/{file[1:]}"
                dbgems.dbutils.fs.cp(source_file, target_file, True)
                print(dbgems.clock_stopped(start))

    def __del_extra_files(self, local_files: List[str]) -> None:
        """
        Remove one file at a time (picking up what was not covered by processing directories)
        :return: None
        """
        for file in local_files:
            if file not in self.remote_files and not file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                print(f"| removing extra file: {file}", end="...")
                dbgems.dbutils.fs.rm(f"{self.datasets_path}/{file[1:]}", True)
                print(dbgems.clock_stopped(start))

    def __add_extra_files(self, local_files: List[str]) -> None:
        """
        Add one file at a time (picking up what was not covered by processing directories)
        :return: None
        """
        for file in self.remote_files:
            if file not in local_files and not file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                print(f"| restoring missing file: {file}", end="...")
                source_file = f"{self.data_source_uri}/{file[1:]}"
                target_file = f"{self.datasets_path}/{file[1:]}"
                dbgems.dbutils.fs.cp(source_file, target_file, True)
                print(dbgems.clock_stopped(start))

    @staticmethod
    def list_r(path: str, prefix: Optional[str] = None, results: Optional[List[str]] = None) -> List[str]:
        """
        Utility method used by the dataset validation, this method performs a recursive list of the specified path and returns the sorted list of paths.
        """
        if prefix is None: prefix = path
        if results is None: results = list()

        try:
            files = dbgems.dbutils.fs.ls(path)
        except:
            files = []

        for file in files:
            data = file.path[len(prefix):]
            results.append(data)
            if file.isDir():
                DatasetManager.list_r(file.path, prefix, results)

        results.sort()
        return results
