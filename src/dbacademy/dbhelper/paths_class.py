class Paths:
    from .lesson_config_class import LessonConfig

    def __init__(self, *, lesson_config: LessonConfig, working_dir_root: str, datasets: str, enable_streaming_support: bool):

        self._working_dir_root = working_dir_root

        if lesson_config.name is None:
            self.working_dir = working_dir_root
        else:
            self.working_dir = f"{self._working_dir_root}/{lesson_config.name}"

        if lesson_config.created_catalog:
            # A little hacky, but if we created the catalog, we don't have a user_db_path
            # because UC will be managing the database location for us
            self.user_db = None
        else:
            self.user_db = f"{self.working_dir}/database.db"

        self.datasets = datasets

        # When working with streams, it helps to put all checkpoints in their
        # own directory relative the previously defined working_dir
        if enable_streaming_support:
            self.checkpoints = f"{self.working_dir}/_checkpoints"

    # noinspection PyGlobalUndefined
    @staticmethod
    def exists(path):
        global dbutils
        from dbacademy_gems import dbgems

        """
        Returns true if the specified path exists else false.
        """
        try:
            return len(dbgems.dbutils.fs.ls(path)) >= 0
        except Exception:
            return False

    def print(self, padding="  ", self_name="self."):
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
