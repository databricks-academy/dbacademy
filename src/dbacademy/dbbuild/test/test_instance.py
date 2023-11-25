__all__ = ["TestInstance"]

from dbacademy.dbbuild.build_config_data import BuildConfigData
from dbacademy.dbbuild.publish.notebook_def import NotebookDef
from dbacademy.dbbuild.test import TestType


class TestInstance:

    def __init__(self, build_config: BuildConfigData, notebook: NotebookDef, test_dir: str, test_type: TestType):
        import hashlib

        self.__build_config = build_config
        self.__notebook = notebook
        self.__job_id = None
        self.__run_id = None
        self.__test_type = test_type

        if notebook.include_solution:
            self.__notebook_path = f"{test_dir}/Solutions/{notebook.path}"
        else:
            self.__notebook_path = f"{test_dir}/{notebook.path}"

        hash_code = str(hashlib.sha256(self.notebook_path.encode()).hexdigest())[-6:]
        test_name = build_config.name.lower().replace(" ", "-")
        self.__job_name = f"[TEST] {test_name} | {test_type} | {hash_code}"

    @property
    def notebook(self) -> NotebookDef:
        return self.__notebook

    @property
    def job_id(self) -> str:
        return self.__job_id

    @job_id.setter
    def job_id(self, job_id: str) -> None:
        self.__job_id = job_id

    @property
    def run_id(self) -> str:
        return self.__run_id

    @run_id.setter
    def run_id(self, run_id: str) -> None:
        self.__run_id = run_id

    @property
    def test_type(self) -> str:
        return self.__test_type

    @property
    def notebook_path(self) -> str:
        return self.__notebook_path

    @property
    def job_name(self) -> str:
        return self.__job_name
