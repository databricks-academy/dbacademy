__all__ = ["DriveApi"]

import io
from typing import Dict, Any
from dbacademy.clients.google.google_client_exception import GoogleClientException


class DriveApi:

    def __init__(self, service_account_info: Dict[str, Any]):
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

        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        scoped_credentials = credentials.with_scopes(scopes)

        self.__drive_service = googleapiclient.discovery.build("drive", "v3", credentials=scoped_credentials)

    @property
    def drive_service(self):
        return self.__drive_service

    @staticmethod
    def to_file_name(file: Dict[str, str]) -> str:
        name = file.get("name")
        file_name = f"{name}.pdf".replace(":", "-").replace(" ", "-").lower()

        while "--" in file_name:
            file_name = file_name.replace("--", "-")

        return file_name

    @staticmethod
    def to_gdoc_id(*, gdoc_id: str = None, gdoc_url: str = None):
        assert gdoc_id or gdoc_url, f"One of the two parameters (gdoc_id or gdoc_url) must be specified."

        if not gdoc_id and gdoc_url:
            gdoc_id = gdoc_url.split("/")[-2]

        return gdoc_id

    def file_get(self, file_id: str) -> Dict[str, Any]:
        request = self.drive_service.files().get(fileId=file_id)
        return self.execute(request)

    def file_export(self, file_id: str) -> io.BytesIO:
        import io

        # noinspection PyPackageRequirements
        from googleapiclient.http import MediaIoBaseDownload

        drive_service = self.drive_service

        class DownloadRequest:
            def __init__(self):
                self.file_bytes = None

            def execute(self):
                export_request = drive_service.files().export_media(fileId=file_id, mimeType='application/pdf')

                self.file_bytes = io.BytesIO()
                downloader = MediaIoBaseDownload(self.file_bytes, export_request)

                done = False
                while done is False:
                    status, done = downloader.next_chunk()

        request = DownloadRequest()
        self.execute(request)

        return request.file_bytes

    def file_copy(self, *, file_id: str, name: str, parent_folder_id: str) -> Dict[str, Any]:
        params = {
            "parents": [parent_folder_id],
            "name": name
        }
        request = self.drive_service.files().copy(fileId=file_id, body=params)
        return self.execute(request)

    def file_delete(self, file_id: str) -> None:
        try:
            request = self.drive_service.files().delete(fileId=file_id)
            self.execute(request)
        except GoogleClientException as e:
            raise GoogleClientException(0, f"Failed to delete Google Drive resource (https://drive.google.com/drive/folders/{file_id})\n{e.message}") from e

    def folder_delete(self, folder_id: str) -> None:
        return self.file_delete(folder_id)

    def folder_list(self, folder_id):
        request = self.drive_service.files().list(q=f"'{folder_id}' in parents")
        response = self.execute(request)
        return response.get("files")

    def folder_create(self, parent_folder_id: str, folder_name: str) -> Dict[str, Any]:
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id]
        }
        request = self.drive_service.files().create(body=file_metadata)
        return self.execute(request)

    @staticmethod
    def execute(request) -> Dict[str, Any]:
        import json, socket

        # noinspection PyPackageRequirements
        import googleapiclient.errors

        try:
            return request.execute()

        except socket.timeout:
            raise GoogleClientException(0, f"The request has timed out")

        except googleapiclient.errors.HttpError as e:
            if e.resp.get("content-type", "").startswith('application/json'):
                errors = json.loads(e.content).get("error", {}).get("errors")
                first_error = errors[0] if len(errors) > 0 else [{"message": str(e)}]
                reason = first_error.get("message")
                message = str(reason)
                raise GoogleClientException(e.status_code, message)
            else:
                raise GoogleClientException(e.status_code, f"{type(e)} {e}")

        except Exception as e:
            raise GoogleClientException(0, f"{type(e)} {e}")
