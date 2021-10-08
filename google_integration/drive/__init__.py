from pathlib import Path
from typing import Optional, Union

from googleapiclient.http import MediaFileUpload

from google_integration.base import BaseGoogleService
from google_integration.utils.drive import query_constructor
from constants import (
    GOOGLE_DRIVE_SCOPES,
    GOOGLE_DRIVE_DEFAULT_PAGE_SIZE,
    GOOGLE_DRIVE_MIME_TYPES,
    GOOGLE_DRIVE_ROOT_FOLDER_NAME
)

# configure what scopes to use - see constants.py for reference
USED_SCOPES = ("full", )


class GoogleDriveService(BaseGoogleService):

    default_page_size: int

    def __init__(self, support_all_drives: bool = False, drive_id: str = None, *args, **kwargs):
        scopes = [GOOGLE_DRIVE_SCOPES.get(scope) for scope in USED_SCOPES]
        self.default_page_size = GOOGLE_DRIVE_DEFAULT_PAGE_SIZE
        self.support_all_drives = support_all_drives
        self.drive_id = drive_id
        super().__init__(scopes=scopes, service_name="drive", version="v3", *args, **kwargs)

    def _list_resources(self, query, page_size: Optional[int] = None, limit: Optional[int] = None) -> list:
        """List all resources filtered with query.

        :param query: query string
        :param page_size: number of items to be returned
        :param limit: max number of records to be fetched
        :return: tuple - next page token, and items returned by the query
        """
        resources: list = []
        while True:
            result = self.service.files().list(
                pageSize=page_size or self.default_page_size,
                q=query,
                fields="nextPageToken, files",
                orderBy="createdTime",
                supportsAllDrives=self.support_all_drives,
                driveId=self.drive_id,
                corpora="drive",
                includeItemsFromAllDrives=True
            ).execute()
            resources += result.get("files", [])

            next_page_token = result.get("nextPageToken")
            if not next_page_token or (limit and len(resources) > limit):
                break

        return resources[:limit]

    def _log_items(self, items: list[dict]) -> None:
        """Log items by printing their name and ID

        :param items: resource items as a list of dicts
        :return:
        """
        if not items:
            self.logger.warning("No items returned as a response!")
        else:
            self.logger.debug("Got response from the target with the following items:")
            for item in items:
                self.logger.debug(f"Name: {item['name']}\tItem ID: {item['id']}")

    def get_files(self, parent_id: Optional[str] = None) -> list[dict]:
        """Get all files in the entered path that have mime type - file

        :param parent_id: folder parent identifier

        :return: list of files
        """
        query = query_constructor(mime_type=GOOGLE_DRIVE_MIME_TYPES['file'], parent_id=parent_id)
        items = self._list_resources(query)
        self._log_items(items)

        return items

    def get_folders(self, parent_id: Optional[str] = None) -> list[dict]:
        """Get all folders in the entered path that have mime type - folder

        :param parent_id: folder parent identifier

        :return: list of folders
        """
        query = query_constructor(mime_type=GOOGLE_DRIVE_MIME_TYPES['folder'], parent_id=parent_id)
        items = self._list_resources(query)

        self._log_items(items)

        return items

    def upload_file(self, path: Union[str, Path], mime_type: str, g_name: str, g_mime_type: Optional[str], parent_id: Optional[str]) -> str:
        """Method to upload the files to the Google Drive

        :param path: local file path to be uploaded
        :param parent_id: parent ID for the folder
        :param g_name: filename to be saved in the Google Drive
        :param g_mime_type: optional mime type that file has to be converted to
        :param mime_type: file mime type
        :return: uploaded file ID
        """
        parent_id = parent_id or GOOGLE_DRIVE_ROOT_FOLDER_NAME

        file_metadata = {
            'name': g_name,
            'parents': [parent_id]
        }
        if g_mime_type:
            file_metadata["mimeType"] = g_mime_type

        media = MediaFileUpload(path, mimetype=mime_type)
        file_id = self._create_resource(file_metadata, media)
        self.logger.info("File was uploaded successfully.")

        return file_id

    def _create_resource(self, resource_metadata: dict, media_body: Optional[MediaFileUpload] = None) -> str:
        """Creates resource with provided metadata and other attributes

        :param resource_metadata: resource metadata as a dictionary that will be send to the target
        :param media_body: media body that would be uploaded to the Google Drive
        :return: resource ID
        """
        resource = self.service.files().create(
            body=resource_metadata,
            fields='id',
            media_body=media_body,
            supportsAllDrives=self.support_all_drives,
        ).execute()

        return resource['id']

    def get_folder_id(self, folder_name: str, parent_id: Optional[str] = None) -> Optional[str]:
        """Method to get folder ID by name

        :param folder_name: folder name for look up
        :param parent_id:
        :return: folder ID if found
        """
        parent_id = parent_id or GOOGLE_DRIVE_ROOT_FOLDER_NAME
        query = query_constructor(mime_type=GOOGLE_DRIVE_MIME_TYPES['folder'], parent_id=parent_id, name=folder_name)

        items = self._list_resources(query)
        if not items:
            self.logger.warning(f"Query returned no folder with desired name.")
        elif len(items) > 2:
            self.logger.warning(f"There are more than one folder with the same name for the parent {parent_id}.")
        else:
            # as there's only one item, get it and return the folder ID
            folder = items[0]
            return folder['id']

        return None

    def get_or_create_folder_id(self, folder_name: str, parent_id: Optional[str] = None) -> str:
        """ Returns the folder ID for entered name.
        If there's no folder inside parent folder provided, create new one.

        :param folder_name: folder name ID required for
        :param parent_id: parent ID. Empty parent ID would be translated to the root directory name
        :return: folder ID
        """
        parent_id = parent_id or GOOGLE_DRIVE_ROOT_FOLDER_NAME
        folder_id = self.get_folder_id(folder_name, parent_id)
        # if there's no folder created with that name - create new
        if not folder_id:
            folder_metadata = {
                "name": folder_name,
                "mimeType": GOOGLE_DRIVE_MIME_TYPES['folder'],
                "parents": [parent_id]
            }
            folder_id = self._create_resource(folder_metadata)

        return folder_id

    def get_path_parent_id(self, path: str, splitter: str = '/') -> str:
        """Method to get parent ID for provided path

        :param path: absolute path to the directory
        :param splitter: path splitter
        :return: parent ID to the last folder in the path
        """
        parents: list[str] = path.split(splitter)
        # Set parent to the root ID (drive ID for shared drive)
        parent_id: Optional[str] = self.drive_id
        for folder_name in parents:
            if not folder_name:
                continue
            parent_id = self.get_or_create_folder_id(folder_name, parent_id)

        return parent_id
