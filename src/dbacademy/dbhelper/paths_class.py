class Paths:
    from dbacademy.dbhelper.lesson_config_class import LessonConfig

    def __init__(self, *, _lesson_config: LessonConfig, _working_dir_root: str, _datasets: str, _archives: str, _convert_to_vm_path: bool):

        self.__working_dir_root = _working_dir_root
        self.__convert_to_vm_path = _convert_to_vm_path

        if _lesson_config.name is None:
            self.__working_dir = _working_dir_root
        else:
            self.__working_dir = f"{_working_dir_root}/{_lesson_config.name}"

        if _lesson_config.create_catalog:
            # A little hacky, but if we created the catalog, we don't have a user_db_path
            # because UC will be managing the database location for us
            self.__user_db = None
        else:
            self.__user_db = f"{self.__working_dir}/database.db"

        self.__datasets = _datasets
        self.__archives = _archives

        # When working with streams, it helps to put all checkpoints in their
        # own directory relative the previously defined working_dir
        if _lesson_config.enable_streaming_support:
            self.checkpoints = f"{self.__working_dir}/_checkpoints"

    @property
    def convert_to_vm_path(self) -> bool:
        return self.__convert_to_vm_path

    @property
    def user_db(self) -> str:
        return self.to_vm_path(self.__user_db) if self.convert_to_vm_path else self.__user_db

    @user_db.setter
    def user_db(self, value: str) -> None:
        self.__user_db = value

    @property
    def working_dir_root(self) -> str:
        return self.to_vm_path(self.__working_dir_root) if self.convert_to_vm_path else self.__working_dir_root

    @working_dir_root.setter
    def working_dir_root(self, value: str) -> None:
        self.working_dir_root = value

    @property
    def working_dir(self) -> str:
        return self.to_vm_path(self.__working_dir) if self.convert_to_vm_path else self.__working_dir

    @working_dir.setter
    def working_dir(self, value: str) -> None:
        self.working_dir = value

    @property
    def datasets(self) -> str:
        return self.to_vm_path(self.__datasets) if self.convert_to_vm_path else self.__datasets

    @datasets.setter
    def datasets(self, value: str) -> None:
        self.datasets = value

    @property
    def archives(self) -> str:
        return self.to_vm_path(self.__archives) if self.convert_to_vm_path else self.__archives

    @archives.setter
    def archives(self, value: str) -> None:
        self.archives = value

    @classmethod
    def to_vm_path(cls, _path: str) -> str:
        return None if _path is None else _path.replace("dbfs:/", "/dbfs/")

    @classmethod
    def exists(cls, path: str) -> bool:
        """
        Returns true if the specified path exists else false.
        """
        from dbacademy import dbgems
        try:
            return len(dbgems.dbutils.fs.ls(path)) >= 0
        except Exception:
            return False

    def print(self, padding="| ", self_name="self."):
        """
        Prints all the paths attached to this instance of Paths
        """
        max_key_len = 0
        for key in self.__dict__:
            if not key.startswith("_"):
                max_key_len = len(key) if len(key) > max_key_len else max_key_len

        for key in self.__dict__:
            if not key.startswith("_"):
                label = f"{padding}{self_name}paths.{key}: "
                if self.__dict__[key] is not None:
                    print(label.ljust(max_key_len + 13) + self.__dict__[key])

    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n").replace("{", "").replace("}", "").replace("'", "")
