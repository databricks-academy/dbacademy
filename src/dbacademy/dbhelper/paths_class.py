class Paths:
    from dbacademy.dbhelper.lesson_config_class import LessonConfig

    def __init__(self, *, _lesson_config: LessonConfig, _working_dir_root: str, _datasets: str, _archives: str):

        if _lesson_config.name is None:
            self.working_dir = f"{_working_dir_root}/working"
        else:
            self.working_dir = f"{_working_dir_root}/{_lesson_config.name}"

        if _lesson_config.create_catalog:
            # A little hacky, but if we created the catalog, we don't have a user_db_path
            # because UC will be managing the database location for us
            self.user_db = None
        else:
            self.user_db = f"{self.working_dir}/database.db"

        self.datasets = _datasets
        self.__archives = _archives

        # When working with streams, it helps to put all checkpoints in their
        # own directory relative the previously defined working_dir
        if _lesson_config.enable_streaming_support:
            self.checkpoints = f"{self.working_dir}/_checkpoints"

    @property
    def archives(self) -> str:
        return self.__archives

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
