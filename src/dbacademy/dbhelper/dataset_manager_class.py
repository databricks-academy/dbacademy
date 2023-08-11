from typing import Optional, List


class DatasetManager:
    from dbacademy.dbhelper import DBAcademyHelper

    @staticmethod
    def from_dbacademy_helper(da: DBAcademyHelper):

        if da.paths.datasets is None and da.paths.archives is None:
            raise ValueError(f"""One of the two parameters, "DBAcademyHelper.paths.datasets" or "DBAcademyHelper.paths.archives", must be specified.""")
        elif da.paths.archives is not None:
            # Prefer the archives path over the datasets path
            install_path = da.paths.archives
        else:
            install_path = da.paths.datasets

        return DatasetManager(_data_source_uri=da.data_source_uri,
                              _staging_source_uri=da.staging_source_uri,
                              _datasets_path=da.paths.datasets,
                              _archives_path=da.paths.archives,
                              _install_path=install_path,
                              _install_min_time=da.course_config.install_min_time,
                              _install_max_time=da.course_config.install_max_time,
                              _remote_files=da.course_config.remote_files)

    def __init__(self, *,
                 _data_source_uri: str,
                 _staging_source_uri: Optional[str],
                 _datasets_path: Optional[str],
                 _archives_path: Optional[str],
                 _install_path: str,
                 _install_min_time: Optional[str],
                 _install_max_time: Optional[str],
                 _remote_files: List[str]):
        """
        Creates an instance of DatasetManager
        :param _data_source_uri: See DBAcademy.data_source_uri
        :param _staging_source_uri: See DBAcademy.staging_source_uri
        :param _install_path: See DBAcademy.paths.archives and DBAcademy.paths.datasets, Paths
        :param _install_min_time: See CourseConfig.install_min_time, str
        :param _install_max_time: See CourseConfig.install_max_time, str
        """
        self.__fixes = 0
        self.__repaired_paths = []

        self.__data_source_uri = _data_source_uri
        self.__staging_source_uri = _staging_source_uri

        self.__remote_files = _remote_files

        self.__datasets_path = _datasets_path
        self.__archives_path = _archives_path

        self.__install_path = _install_path
        self.__install_min_time = _install_min_time
        self.__install_max_time = _install_max_time

    @property
    def datasets_path(self) -> str:
        return self.__datasets_path

    @property
    def archives_path(self) -> str:
        return self.__archives_path

    @property
    def install_path(self) -> str:
        return self.__install_path

    @property
    def install_min_time(self) -> Optional[str]:
        return self.__install_min_time

    @property
    def install_max_time(self) -> Optional[str]:
        return self.__install_max_time

    @property
    def remote_files(self) -> List[str]:
        return self.__remote_files

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

    def install_dataset(self, *, reinstall_datasets: bool) -> None:
        """
        Install the datasets used by this course to DBFS.

        This ensures that data and compute are in the same region which subsequently mitigates performance issues
        when the storage and compute are, for example, on opposite sides of the world.
        """
        from dbacademy import dbgems
        from dbacademy.dbhelper.paths_class import Paths
        # if not repairing_dataset: print(f"\nThe source for the datasets is\n{self.data_source_uri}/")
        # if not repairing_dataset: print(f"\nYour local dataset directory is {datasets_path}")

        action = "Install" if self.archives_path is None else "Download"

        if Paths.exists(self.install_path):
            # It's already installed...
            if reinstall_datasets:
                if self.archives_path is None:
                    # This is a classic installation with shared datasets
                    print(f"\nRemoving previously installed shared datasets")
                    dbgems.dbutils.fs.rm(self.install_path, True)
                else:
                    # This is an installation from an archive with per-user datasets
                    print(f"\nRemoving previously installed private datasets")
                    dbgems.dbutils.fs.rm(self.archives_path, True)

            else:  # not reinstall_datasets:
                print(f"\nSkipping {action.lower()} of existing datasets to \"{self.install_path}\"")
                self.validate_datasets(fail_fast=False)
                self.unpack_archive()
                return  # All done.

        print(f"\n{action}ing datasets:")
        print(f"| from \"{self.data_source_uri}\"")
        print(f"| to   \"{self.install_path}\"")
        if self.install_min_time is not None and self.install_max_time is not None:
            print(f"|")
            print(f"| NOTE: The datasets that we are installing are located in Washington, USA - depending on the")
            print(f"|       region that your workspace is in, this operation can take as little as {self.install_min_time} and")
            print(f"|       upwards to {self.install_max_time}, but this is a one-time operation.")

        # Using data_source_uri is a temporary hack because it assumes we can actually
        # reach the remote repository - in cases where it's blocked, this will fail.
        files = dbgems.dbutils.fs.ls(self.data_source_uri)

        total = len(files)
        print("| ")

        install_start = dbgems.clock_start()

        for i, f in enumerate(files):
            start = dbgems.clock_start()
            name = f.name[:-1] if f.name.endswith("/") else f.name
            print(f"| copying {i + 1}/{total}: {name}", end="...")

            source_path = f"{self.data_source_uri}/{name}"
            target_path = f"{self.install_path}/{name}"

            dbgems.dbutils.fs.cp(source_path, target_path, True)
            print(dbgems.clock_stopped(start))

        print(f"|\n| completed datasets installation successfully...{dbgems.clock_stopped(install_start)}")

        self.validate_datasets(fail_fast=False)
        self.unpack_archive()

    def unpack_archive(self) -> None:
        import shutil
        from dbacademy import dbgems
        from dbacademy.dbhelper.paths_class import Paths

        if self.archives_path is None:
            print(f"| archives_path = {self.archives_path}")
            return  # This is a classic install, nothing to unpack

        try:
            files = list() if not Paths.exists(self.datasets_path) else dbgems.dbutils.fs.ls(self.datasets_path)
        except:
            files = list()

        if len(files) > 0:
            print(f"| Skipping install of existing datasets to \"{self.datasets_path}\"")
        else:
            print(f"| Installing datasets to \"{self.datasets_path}\"")
            archive_path = f"{self.archives_path}/archive.zip".replace("dbfs:/", '/dbfs/')
            dataset_path = self.datasets_path.replace("dbfs:/", '/dbfs/')
            shutil.unpack_archive(archive_path, dataset_path)

    def validate_datasets(self, fail_fast: bool) -> None:
        """
        Validates the "install" of the datasets by recursively listing all files in the remote data repository as well as the local data repository, validating that each file exists but DOES NOT validate file size or checksum.
        """
        from dbacademy import dbgems

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
        from dbacademy import dbgems

        print("| listing local files", end="...")
        start = dbgems.clock_start()

        local_files = DatasetManager.list_r(self.install_path)
        print(dbgems.clock_stopped(start))

        # print(f"\n| " + ("*"*80))
        # print(f"| install_path: {self.install_path}")
        # print(f"| local_files:  {local_files}")
        # print(f"| " + ("*"*80) + "\n")
        # print()

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
        from dbacademy import dbgems

        for file in local_files:
            if file not in self.remote_files and file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                self.repaired_paths.append(file)
                print(f"| removing extra path: {file}", end="...")
                dbgems.dbutils.fs.rm(f"{self.install_path}/{file[1:]}", True)
                print(dbgems.clock_stopped(start))

    def __add_extra_paths(self, local_files: List[str]) -> None:
        """
        Adds extra directories (cascade effect vs one file at a time)
        :return: None
        """
        from dbacademy import dbgems

        for file in self.remote_files:
            if file not in local_files and file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                self.repaired_paths.append(file)
                print(f"| restoring missing path: {file}", end="...")
                source_file = f"{self.data_source_uri}/{file[1:]}"
                target_file = f"{self.install_path}/{file[1:]}"
                dbgems.dbutils.fs.cp(source_file, target_file, True)
                print(dbgems.clock_stopped(start))

    def __del_extra_files(self, local_files: List[str]) -> None:
        """
        Remove one file at a time (picking up what was not covered by processing directories)
        :return: None
        """
        from dbacademy import dbgems

        for file in local_files:
            if file not in self.remote_files and not file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                print(f"| removing extra file: {file}", end="...")
                dbgems.dbutils.fs.rm(f"{self.install_path}/{file[1:]}", True)
                print(dbgems.clock_stopped(start))

    def __add_extra_files(self, local_files: List[str]) -> None:
        """
        Add one file at a time (picking up what was not covered by processing directories)
        :return: None
        """
        from dbacademy import dbgems

        for file in self.remote_files:
            if file not in local_files and not file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                print(f"| restoring missing file: {file}", end="...")
                source_file = f"{self.data_source_uri}/{file[1:]}"
                target_file = f"{self.install_path}/{file[1:]}"
                dbgems.dbutils.fs.cp(source_file, target_file, True)
                print(dbgems.clock_stopped(start))

    @classmethod
    def list_r(cls, path: str, prefix: Optional[str] = None, results: Optional[List[str]] = None) -> List[str]:
        """
        Utility method used by the dataset validation, this method performs a recursive list of the specified path and returns the sorted list of paths.
        """
        from dbacademy import dbgems

        if prefix is None:
            prefix = path
        if results is None:
            results = list()

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
