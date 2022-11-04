from dbacademy import dbgems


# noinspection PyPackageRequirements
class DocsPublisher:
    from dbacademy.dbbuild.publish.publishing_info_class import Translation

    def __init__(self, build_name: str, version: str, translation: Translation):
        self.__translation = translation
        self.__build_name = build_name
        self.__version = version
        self.__pdfs = dict()

        try:
            import google
            import googleapiclient
        except ModuleNotFoundError:
            raise Exception("The following libraries are required, runtime-dependencies not bundled with the dbacademy library: google-api-python-client google-auth-httplib2 google-auth-oauthlib")

    @property
    def translation(self):
        return self.__translation

    @property
    def build_name(self):
        return self.__build_name

    @property
    def version(self):
        return self.__version

    @staticmethod
    def __get_service(api_name, api_version):
        import json

        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/drive",
                  "https://www.googleapis.com/auth/drive.file",
                  "https://www.googleapis.com/auth/drive.readonly",
                  "https://www.googleapis.com/auth/drive.metadata.readonly",
                  "https://www.googleapis.com/auth/drive.appdata",
                  "https://www.googleapis.com/auth/drive.metadata",
                  "https://www.googleapis.com/auth/drive.photos.readonly",

                  "https://www.googleapis.com/auth/presentations.readonly"]

        service_account_info = dbgems.dbutils.secrets.get("gcp-prod-curriculum", "service-account-info")
        service_account_info = json.loads(service_account_info)

        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        scoped_credentials = credentials.with_scopes(scopes)
        return build(api_name, api_version, credentials=scoped_credentials)

    def __download_doc(self, *, index: int, total: int, gdoc_id: str = None, gdoc_url: str = None):
        import os, io, shutil
        from googleapiclient.http import MediaIoBaseDownload
        from dbacademy import dbgems

        assert gdoc_id or gdoc_url, f"One of the two parameters (gdoc_id or gdoc_url) must be specified."
        if not gdoc_id and gdoc_url:
            gdoc_id = gdoc_url.split("/")[-2]

        drive_service = self.__get_service("drive", "v3")

        file = drive_service.files().get(fileId=gdoc_id).execute()
        name = file.get("name")
        file_name = f"{name}.pdf".replace(":", "-").replace(" ", "-").lower()
        while "--" in file_name: file_name = file_name.replace("--", "-")
        print(f"\nProcessing {index + 1} or {total}: {name}")

        request = drive_service.files().export_media(fileId=gdoc_id, mimeType='application/pdf')

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk(num_retries=3)
            percent = int(status.progress() * 100)
            print(f"| download {percent}%.")
        print("| download 100%.")

        target_dir = "/dbfs/FileStore/tmp"
        temp_path = f"{target_dir}/{file_name}"
        dist_path = f"/dbfs/mnt/secured.training.databricks.com/distributions/{self.build_name}/v{self.version}/{file_name}"

        if os.path.exists(temp_path): os.remove(temp_path)
        if not os.path.exists(target_dir): os.mkdir(target_dir)

        fh.seek(0)
        with open(temp_path, "wb") as f:
            print(f"| writing {temp_path}")
            shutil.copyfileobj(fh, f)

        fh.seek(0)
        with open(dist_path, "wb") as f:
            print(f"| writing {dist_path}")
            shutil.copyfileobj(fh, f)

        parts = dbgems.get_workspace_url().split("/")
        del parts[-1]
        workspace_url = "/".join(parts)

        return file_name, f"{workspace_url}/files/tmp/{file_name}"

    def process_pdfs(self):

        total = len(self.translation.document_links)

        for index, link in enumerate(self.translation.document_links):
            file_name, file_url = self.__download_doc(index=index, total=total, gdoc_url=link)
            self.__pdfs[file_name] = file_url

    @staticmethod
    def process_google_slides():
        dbgems.print_warning("Not Implemented", "DocsPublisher.process_google_slides() has not yet been implemented")

    def to_html(self):
        html = """<html><body style="font-size:16px">"""

        html += "\nDownloads<ul>"
        for file_name, file_url in self.__pdfs.items():
            html += f"""\n<li><a href="{file_url}">Download {file_name}</a></li>"""
        html += "\n</ul>"

        html += "\nTools/Docs<ul>"
        html += f"""\n<li><a href="{self.translation.published_docs_folder}" target="_blank">Published Folder</a></li>"""
        html += f"""\n<li><a href="{self.translation.publishing_script}" target="_blank">Publishing Script</a></li>"""

        links = self.translation.document_links

        if len(links) == 0:
            html += "\n<li>Documents: None</li>"
        elif len(links) == 1:
            html += f"""\n<li><a href="{links[0]}" target="_blank">Document</a></li>"""
        else:
            html += """\n<li>Documents:</li>"""
            html += """\n<ul style="margin:0">"""
            for i, link in enumerate(links):
                html += f"""\n<li><a href="{link}" target="_blank">Document #{i + 1}</a></li>"""
            html += "\n</ul>"

        html += """\n</body></html>"""
        return html
