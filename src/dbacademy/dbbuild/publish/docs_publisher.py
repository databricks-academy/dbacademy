from typing import Dict
from dbacademy import dbgems


class DocsPublisher:
    from dbacademy.dbbuild.publish.publishing_info_class import Translation

    def __init__(self, build_name: str, version: str, translation: Translation):
        self.__translation = translation
        self.__build_name = build_name
        self.__version = version
        self.__pdfs = dict()
        self.__drive_service = self.__get_service("drive", "v3")

        try:
            # noinspection PyPackageRequirements
            import google
            # noinspection PyPackageRequirements
            import googleapiclient
        except ModuleNotFoundError:
            raise Exception("The following libraries are required, runtime-dependencies not bundled with the dbacademy library: google-api-python-client google-auth-httplib2 google-auth-oauthlib")

    @property
    def translation(self) -> Translation:
        return self.__translation

    @property
    def build_name(self) -> str:
        return self.__build_name

    @property
    def version(self) -> str:
        return self.__version

    @staticmethod
    def __get_service(api_name, api_version):
        import json

        # noinspection PyPackageRequirements
        from google.oauth2 import service_account
        # noinspection PyPackageRequirements
        import googleapiclient.discovery

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
        return googleapiclient.discovery.build(api_name, api_version, credentials=scoped_credentials)

    @staticmethod
    def __to_gdoc_id(gdoc_url):
        return gdoc_url.split("/")[-2]

    def get_file(self, gdoc_id: str = None, gdoc_url: str = None) -> Dict[str, str]:

        assert gdoc_id or gdoc_url, f"One of the two parameters (gdoc_id or gdoc_url) must be specified."

        if not gdoc_id and gdoc_url:
            gdoc_id = self.__to_gdoc_id(gdoc_url)

        return self.__drive_service.files().get(fileId=gdoc_id).execute()

    @staticmethod
    def to_file_name(file: Dict[str, str]) -> str:
        name = file.get("name")
        file_name = f"{name}.pdf".replace(":", "-").replace(" ", "-").lower()
        while "--" in file_name: file_name = file_name.replace("--", "-")

        return file_name

    def get_distribution_path(self, file: Dict[str, str]) -> str:
        file_name = self.to_file_name(file)
        return f"/dbfs/mnt/secured.training.databricks.com/distributions/{self.build_name}/v{self.version}/{file_name}"

    def __download_doc(self, *, index: int, total: int, gdoc_id: str = None, gdoc_url: str = None) -> (str, str):
        import os, io, shutil
        # noinspection PyPackageRequirements
        from googleapiclient.http import MediaIoBaseDownload
        from dbacademy import dbgems

        file = self.get_file(gdoc_id, gdoc_url)
        name = file.get("name")
        file_name = self.to_file_name(file)

        print(f"\nProcessing {index + 1} or {total}: {name}")

        request = self.__drive_service.files().export_media(fileId=gdoc_id, mimeType='application/pdf')

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
        dist_path = self.get_distribution_path(file)

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

    def process_pdfs(self) -> None:

        total = len(self.translation.document_links)

        for index, link in enumerate(self.translation.document_links):
            try:
                file_name, file_url = self.__download_doc(index=index, total=total, gdoc_url=link)
                self.__pdfs[file_name] = file_url
            except Exception as e:
                dbgems.print_warning("SKIPPING - CANNOT DOWNLOAD", f"Document {index+1} of {total} ({link}) cannot be downloaded and the publishing of this doc is being skipped.")
                print(e)

    def process_google_slides(self) -> None:
        parent_folder_id = self.translation.published_docs_folder.split("/")[-1]
        files = self.__drive_service.files().list(q=f"'{parent_folder_id}' in parents").execute().get("files")
        files = [f for f in files if f.get("name") == f"v{self.version}"]

        if len(files) == 0:
            folder_name = f"v{self.version}"
            file_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id]
            }
            folder = self.__drive_service.files().create(body=file_metadata).execute()
            print(f"Created folder", end=" ")
        else:
            folder = files[0]
            print(f"Existing folder", end=" ")

        folder_id = folder.get("id")
        folder_name = folder.get("name")
        print(f"{folder_name} ({folder_id})")

        total = len(self.translation.document_links)
        for index, link in enumerate(self.translation.document_links):
            gdoc_id = self.__to_gdoc_id(link)
            file = self.__drive_service.files().get(fileId=gdoc_id).execute()
            name = file.get("name")

            print(f"Copying {index + 1} of {total}: {name}")

            params = {
                "parents": [folder_id],
                "name": name
            }
            self.__drive_service.files().copy(fileId=gdoc_id, body=params).execute()

    def to_html(self) -> str:
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
