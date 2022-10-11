class DevHelper:
    from dbacademy_helper import DBAcademyHelper

    def __init__(self, da: DBAcademyHelper):

        self.da = da
        self.client = da.client

    def enumerate_remote_datasets(self):
        """
        Development function used to enumerate the remote datasets for use in validate_datasets()
        """
        from dbacademy_gems import dbgems

        files = self.da.list_r(self.da.data_source_uri)
        files = "remote_files = " + str(files).replace("'", "\"")

        dbgems.display_html(f"""
            <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
            <textarea rows="10" style="width:100%">{files}</textarea>
        """)

    def enumerate_local_datasets(self):
        """
        Development function used to enumerate the local datasets for use in validate_datasets()
        """
        from dbacademy_gems import dbgems

        files = self.da.list_r(self.da.paths.datasets)
        files = "remote_files = " + str(files).replace("'", "\"")

        dbgems.display_html(f"""
            <p>Copy the following output and paste it in its entirety into cell of the _utility-functions notebook.</p>
            <textarea rows="10" style="width:100%">{files}</textarea>
        """)
