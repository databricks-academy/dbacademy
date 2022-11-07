from typing import Dict
from dbacademy import dbgems


class DocsPublisher:
    import io
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

        if not gdoc_id and gdoc_url: gdoc_id = self.__to_gdoc_id(gdoc_url)
        return self.__drive_service.files().get(fileId=gdoc_id).execute()

    @staticmethod
    def to_file_name(file: Dict[str, str]) -> str:
        name = file.get("name")
        file_name = f"{name}.pdf".replace(":", "-").replace(" ", "-").lower()
        while "--" in file_name: file_name = file_name.replace("--", "-")

        return file_name

    def get_distribution_path(self, *, version: str, file: Dict[str, str]) -> str:
        file_name = self.to_file_name(file)
        return f"/dbfs/mnt/secured.training.databricks.com/distributions/{self.build_name}/v{version}/{file_name}"

    def __download_doc(self, *, index: int, total: int, gdoc_id: str = None, gdoc_url: str = None) -> (str, str):
        import io
        # noinspection PyPackageRequirements
        from googleapiclient.http import MediaIoBaseDownload
        from dbacademy import dbgems

        assert gdoc_id or gdoc_url, f"One of the two parameters (gdoc_id or gdoc_url) must be specified."
        if not gdoc_id and gdoc_url: gdoc_id = self.__to_gdoc_id(gdoc_url)

        file = self.get_file(gdoc_id, gdoc_url)
        name = file.get("name")
        file_name = self.to_file_name(file)

        print(f"| Processing {index + 1} or {total}: {name}")

        request = self.__drive_service.files().export_media(fileId=gdoc_id, mimeType='application/pdf')

        file_bytes = io.BytesIO()
        downloader = MediaIoBaseDownload(file_bytes, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
            percent = int(status.progress() * 100)

        temp_path = f"/dbfs/FileStore/tmp/{file_name}"

        self.__save_pdfs(file_bytes, temp_path)
        self.__save_pdfs(file_bytes, self.get_distribution_path(version="LATEST", file=file))
        self.__save_pdfs(file_bytes, self.get_distribution_path(version=self.version, file=file))

        parts = dbgems.get_workspace_url().split("/")
        del parts[-1]
        workspace_url = "/".join(parts)

        return file_name, f"{workspace_url}/files/tmp/{file_name}"

    @staticmethod
    def __save_pdfs(file_bytes: io.BytesIO, path: str):
        import os, shutil

        if os.path.exists(path):
            os.remove(path)

        target_dir = "/".join(path.split("/")[:-1])
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        file_bytes.seek(0)
        with open(path, "wb") as f:
            print(f"| writing {path}")
            shutil.copyfileobj(file_bytes, f)

    def process_pdfs(self) -> None:
        import json, socket
        # noinspection PyPackageRequirements
        import googleapiclient.errors

        print("Exporting DBCs:")
        total = len(self.translation.document_links)

        for index, link in enumerate(self.translation.document_links):
            error_message = f"Document {index + 1} of {total} cannot be downloaded; publishing of this doc is being skipped.\n{link}"

            try:
                file_name, file_url = self.__download_doc(index=index, total=total, gdoc_url=link)
                self.__pdfs[file_name] = file_url

            except socket.timeout:
                dbgems.print_warning("SKIPPING DOWNLOAD / TIMEOUT", error_message)

            except googleapiclient.errors.HttpError as e:
                if e.resp.get("content-type", "").startswith('application/json'):
                    errors = json.loads(e.content).get("error", {}).get("errors")
                    first_error = errors[0] if len(errors) > 0 else [{"message": str(e)}]
                    reason = first_error.get("message")
                    message = f"{str(reason)}\n{link}"
                    dbgems.print_warning(f"SKIPPING DOWNLOAD / {e.status_code}", message)
                else:
                    error_message += f"\n{type(e)} {e}"
                    dbgems.print_warning("SKIPPING / CANNOT DOWNLOAD", error_message)

            except Exception as e:
                error_message += "\n{type(e)} {e}"
                dbgems.print_warning("SKIPPING / CANNOT DOWNLOAD", error_message)

    def process_google_slides(self) -> None:

        print("Publishing Google Docs:")

        parent_folder_id = self.translation.published_docs_folder.split("/")[-1]
        files = self.__drive_service.files().list(q=f"'{parent_folder_id}' in parents").execute().get("files")
        files = [f for f in files if f.get("name") == f"v{self.version}"]

        for folder in files:
            folder_id = folder.get("id")
            folder_name = folder.get("name")
            self.__drive_service.files().delete(fileId=folder_id).execute()
            print(f"| Deleted existing published folder {folder_name} (https://drive.google.com/drive/folders/{folder_id})")

        file_metadata = {
            "name": f"v{self.version}",
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id]
        }
        folder = self.__drive_service.files().create(body=file_metadata).execute()
        folder_id = folder.get("id")
        folder_name = folder.get("name")
        print(f"| Created new published folder {folder_name} (https://drive.google.com/drive/folders/{folder_id})")

        total = len(self.translation.document_links)
        for index, link in enumerate(self.translation.document_links):
            gdoc_id = self.__to_gdoc_id(link)
            file = self.__drive_service.files().get(fileId=gdoc_id).execute()
            name = file.get("name")

            print(f"| Copying {index + 1} of {total}: {name}")

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
