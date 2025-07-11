import os
import shutil
import uuid
from typing import BinaryIO
from app.interfaces.file_storage_port import FileStoragePort

class LocalFileStorageAdapter(FileStoragePort):
    def __init__(self, upload_dir_base: str):
        self.upload_dir_base = upload_dir_base
        os.makedirs(self.upload_dir_base, exist_ok=True)

    def create_project_directory(self, project_id: str) -> str:
        project_dir = os.path.join(self.upload_dir_base, project_id)
        os.makedirs(project_dir, exist_ok=True)
        return project_dir

    def create_project_sub_directory(self, project_id: str, sub_folder: str) -> str:
        sub_dir = os.path.join(self.upload_dir_base, project_id, sub_folder)
        os.makedirs(sub_dir, exist_ok=True)
        return sub_dir

    def generate_unique_file_path(self, directory: str, original_file_name: str) -> str:
        _, extension = os.path.splitext(original_file_name)
        unique_filename = f"{uuid.uuid4()}{extension}"
        return os.path.join(directory, unique_filename)

    def save_file(self, destination_path: str, file_data: BinaryIO) -> None:
        # Ensure the directory for the destination path exists
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        try:
            with open(destination_path, "wb") as buffer:
                shutil.copyfileobj(file_data, buffer)
        except Exception:
            # Handle potential errors, e.g., disk full, permissions
            # For now, re-raise or log
            # print(f"Error saving file to {destination_path}: {e}")
            raise

    def save_text_file(self, destination_path: str, content: str) -> None:
        # Ensure the directory for the destination path exists
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        try:
            with open(destination_path, "w", encoding="utf-8") as f:
                f.write(content)
        except Exception:
            # print(f"Error saving text file to {destination_path}: {e}")
            raise

    def get_file_size(self, file_path: str) -> int:
        try:
            return os.path.getsize(file_path)
        except FileNotFoundError:
            # Or raise a custom error
            return 0
        except Exception:
            # print(f"Error getting file size for {file_path}: {e}")
            raise

    def get_file_extension(self, file_name: str) -> str:
        _, extension = os.path.splitext(file_name)
        return extension
