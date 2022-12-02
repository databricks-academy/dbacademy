from typing import Optional, List
from dbacademy import dbgems


class DatasetManager:
    def __init__(self, *, data_source_uri: str, datasets_path: str, remote_files: List[str]):
        """
        :param data_source_uri: See DBAcademy.data_source_uri
        :param datasets_path: See DBAcademy.paths.datasets, Paths
        """
        self.__fixes = 0
        self.__repaired_paths = []
        self.__data_source_uri = data_source_uri
        self.__datasets_path = datasets_path
        self.__remote_files = remote_files

        print("| listing local files", end="...")
        start = dbgems.clock_start()
        self.__local_files = DatasetManager.list_r(self.datasets_path)
        print(dbgems.clock_stopped(start))

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
    def local_files(self) -> List[str]:
        return self.__local_files

    @property
    def fixes(self) -> int:
        return self.__fixes

    @property
    def repaired_paths(self) -> List[str]:
        return self.__repaired_paths

    def validate_named_dataset(self, *, fail_fast: bool) -> None:
        """
        Utility to validate the installed dataset against the remote repo - provided specifically for Workspace-Setup jobs
        :param fail_fast: Indicates if the operation should fail with an exception if validation fails
        :return: None
        """

        validation_start = dbgems.clock_start()

        start = dbgems.clock_start()
        print("\nEnumerating remote files", end="...")
        self.__remote_files = DatasetManager.list_r(self.data_source_uri)
        print(dbgems.clock_stopped(start))
        print()

        self.__repair_paths()
        self.__repair_files()

        print(dbgems.clock_stopped(validation_start, " total"))
        print()

        if fail_fast: assert self.fixes == 0, f"Unexpected modifications to source datasets."

    def repair(self) -> None:
        self.__repair_paths()
        self.__repair_files()

    def __repair_paths(self) -> None:
        self.__del_extra_paths()
        self.__add_extra_paths()

    def __repair_files(self) -> None:
        self.__del_extra_files()
        self.__add_extra_files()

    def __dataset_not_fixed(self, test_file: str) -> bool:
        for repaired_path in self.repaired_paths:
            if test_file.startswith(repaired_path):
                return False
        return True

    def __del_extra_paths(self) -> None:
        """
        Removes extra directories (cascade effect vs one file at a time)
        :return: None
        """

        for file in self.local_files:
            if file not in self.remote_files and file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                self.repaired_paths.append(file)
                print(f"| removing extra path: {file}", end="...")
                dbgems.dbutils.fs.rm(f"{self.datasets_path}/{file[1:]}", True)
                print(dbgems.clock_stopped(start))

    def __add_extra_paths(self) -> None:
        """
        Adds extra directories (cascade effect vs one file at a time)
        :return: None
        """
        for file in self.remote_files:
            if file not in self.local_files and file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                self.repaired_paths.append(file)
                print(f"| restoring missing path: {file}", end="...")
                source_file = f"{self.data_source_uri}/{file[1:]}"
                target_file = f"{self.datasets_path}/{file[1:]}"
                dbgems.dbutils.fs.cp(source_file, target_file, True)
                print(dbgems.clock_stopped(start))

    def __del_extra_files(self) -> None:
        """
        Remove one file at a time (picking up what was not covered by processing directories)
        :return: None
        """
        for file in self.local_files:
            if file not in self.remote_files and not file.endswith("/") and self.__dataset_not_fixed(test_file=file):
                self.__fixes += 1
                start = dbgems.clock_start()
                print(f"| removing extra file: {file}", end="...")
                dbgems.dbutils.fs.rm(f"{self.datasets_path}/{file[1:]}", True)
                print(dbgems.clock_stopped(start))

    def __add_extra_files(self) -> None:
        """
        Add one file at a time (picking up what was not covered by processing directories)
        :return: None
        """
        for file in self.remote_files:
            if file not in self.local_files and not file.endswith("/") and self.__dataset_not_fixed(test_file=file):
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

    def print_stats(self) -> None:
        if self.fixes == 1:
            print(f"| fixed 1 issue", end=" ")
        elif self.fixes > 0:
            print(f"| fixed {self.fixes} issues", end=" ")
        else:
            print(f"| completed", end=" ")
