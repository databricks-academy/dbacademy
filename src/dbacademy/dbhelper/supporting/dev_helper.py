__all__ = ["DevHelper"]


class DevHelper:

    def __init__(self, db_academy_helper):
        from dbacademy.common import validate
        from dbacademy.dbhelper.dbacademy_helper import DBAcademyHelper
        from dbacademy.clients.darest import DBAcademyRestClient

        self.__da = validate(db_academy_helper=db_academy_helper).required.as_type(DBAcademyHelper)
        self.__client = validate(client=db_academy_helper.client).required.as_type(DBAcademyRestClient)

    def enumerate_remote_datasets(self):
        """
        Development function used to enumerate the remote datasets for use in validate_datasets()
        """
        from dbacademy import dbgems
        from dbacademy.dbhelper.dataset_manager import DatasetManager

        files = DatasetManager.list_r(self.__da.data_source_uri)
        files = "remote_files = " + str(files).replace("'", "\"")

        dbgems.display_html(f"""
            <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
            <textarea rows="10" style="width:100%">{files}</textarea>
        """)

    def enumerate_local_datasets(self):
        """
        Development function used to enumerate the local datasets for use in validate_datasets()
        """
        from dbacademy import dbgems
        from dbacademy.dbhelper.dataset_manager import DatasetManager

        files = DatasetManager.list_r(self.__da.paths.datasets)
        files = "remote_files = " + str(files).replace("'", "\"")

        dbgems.display_html(f"""
            <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
            <textarea rows="10" style="width:100%">{files}</textarea>
        """)
