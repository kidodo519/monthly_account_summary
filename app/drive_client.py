from __future__ import annotations
import io
import os
import json
from dataclasses import dataclass
from typing import List, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# exe/スクリプト両対応のパス解決（main.py と同じ実装）
def resource_path(rel_path: str) -> str:
    import sys
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, rel_path)

SCOPES = ["https://www.googleapis.com/auth/drive"]

@dataclass
class DriveFile:
    id: str
    name: str
    mimeType: str

def _load_sa_credentials():
    """
    サービスアカウント認証を3段フォールバックで解決:
    1) GOOGLE_APPLICATION_CREDENTIALS = 既存ファイルパス
    2) GOOGLE_APPLICATION_CREDENTIALS_JSON = JSON文字列
    3) exe同梱の app/credentials.json
    """
    # 1) 既存ファイルパス
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and os.path.exists(cred_path):
        return service_account.Credentials.from_service_account_file(
            cred_path, scopes=SCOPES
        )

    # 2) 環境変数に JSON 文字列が直接入っている場合
    cred_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if cred_json:
        try:
            info = json.loads(cred_json)
            return service_account.Credentials.from_service_account_info(
                info, scopes=SCOPES
            )
        except json.JSONDecodeError:
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON が不正なJSONです。")

    # 3) バンドル済み credentials.json（PyInstaller --add-data "app\\credentials.json;app"）
    bundled = resource_path("credentials.json")
    if os.path.exists(bundled):
        return service_account.Credentials.from_service_account_file(
            bundled, scopes=SCOPES
        )

    raise RuntimeError(
        "Service account JSON not found.\n"
        "Set either:\n"
        " - GOOGLE_APPLICATION_CREDENTIALS = <absolute path to JSON>, or\n"
        " - GOOGLE_APPLICATION_CREDENTIALS_JSON = <JSON string>, or\n"
        "Bundle app/credentials.json with the executable."
    )

def _build_service():
    creds = _load_sa_credentials()
    # cache_discovery=False で OK（pyinstaller でも安定）
    return build("drive", "v3", credentials=creds, cache_discovery=False)

class DriveClient:
    def __init__(self):
        self.service = _build_service()

    def list_csv_files(self, folder_id: str, page_size: int = 100) -> List[DriveFile]:
        q = (
            f"'{folder_id}' in parents and trashed = false "
            f"and mimeType != 'application/vnd.google-apps.folder'"
        )
        resp = self.service.files().list(
            q=q,
            spaces="drive",
            fields="files(id,name,mimeType)",
            pageSize=page_size,
            corpora="allDrives",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()
        items = resp.get("files", []) or []
        return [DriveFile(id=i["id"], name=i["name"], mimeType=i.get("mimeType", "")) for i in items]

    def download_file(self, file_id: str, local_path: str, mime_type_hint: Optional[str] = None) -> None:
        req = self.service.files().get_media(fileId=file_id, supportsAllDrives=True)
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, req, chunksize=1024 * 1024)  # 1MB/chunk
            done = False
            while not done:
                status, done = downloader.next_chunk()
                # 進捗ログが欲しければ: print(f"Download {int(status.progress() * 100)}%")

    def move_file(self, file_id: str, dest_folder_id: str) -> None:
        meta = self.service.files().get(
            fileId=file_id, fields="parents", supportsAllDrives=True
        ).execute()
        parents = ",".join(meta.get("parents", []))
        self.service.files().update(
            fileId=file_id,
            addParents=dest_folder_id,
            removeParents=parents,
            fields="id, parents",
            supportsAllDrives=True,
        ).execute()
