from typing import Dict, Any
from dbacademy import dbgems

# noinspection PyPackageRequirements
from google.oauth2 import service_account

# noinspection PyPackageRequirements
import googleapiclient.discovery

# noinspection PyPackageRequirements
from googleapiclient.http import MediaIoBaseDownload


class GoogleClient:
    import io

    def __init__(self):
        self.__drive_service = self.__get_service("drive", "v3")

    @staticmethod
    def __get_service(api_name, api_version):
        import json

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

    @property
    def drive_service(self):
        return self.__drive_service

    @staticmethod
    def to_file_name(file: Dict[str, str]) -> str:
        name = file.get("name")
        file_name = f"{name}.pdf".replace(":", "-").replace(" ", "-").lower()
        while "--" in file_name: file_name = file_name.replace("--", "-")

        return file_name

    @staticmethod
    def to_gdoc_id(*, gdoc_id: str = None, gdoc_url: str = None):
        assert gdoc_id or gdoc_url, f"One of the two parameters (gdoc_id or gdoc_url) must be specified."

        if not gdoc_id and gdoc_url:
            gdoc_id = gdoc_url.split("/")[-2]

        return gdoc_id

    def file_get(self, file_id: str) -> Dict[str, Any]:
        return self.__drive_service.files().get(fileId=file_id).execute()

    def file_export(self, file_id: str) -> io.BytesIO:
        import io

        request = self.__drive_service.files().export_media(fileId=file_id, mimeType='application/pdf')

        file_bytes = io.BytesIO()
        downloader = MediaIoBaseDownload(file_bytes, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()

        return file_bytes

    def file_delete(self, folder_id: str) -> None:
        self.__drive_service.files().delete(fileId=folder_id).execute()

    def file_copy(self, *, file_id: str, name: str, parent_folder_id: str) -> Dict[str, Any]:
        params = {
            "parents": [parent_folder_id],
            "name": name
        }
        return self.__drive_service.files().copy(fileId=file_id, body=params).execute()

    def folder_list(self, folder_id):
        response = self.drive_service.files().list(q=f"'{folder_id}' in parents").execute()
        return response.get("files")

    def folder_create(self, parent_folder_id: str, folder_name: str) -> Dict[str, Any]:
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id]
        }
        return self.__drive_service.files().create(body=file_metadata).execute()
