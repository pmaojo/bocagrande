from abc import ABC, abstractmethod
from typing import BinaryIO

class FileStoragePort(ABC):
    @abstractmethod
    def save_file(self, destination_path: str, file_data: BinaryIO) -> None:
        """Saves binary file data to the specified destination path."""
        pass

    @abstractmethod
    def save_text_file(self, destination_path: str, content: str) -> None:
        """Saves text content to the specified destination path."""
        pass

    @abstractmethod
    def create_project_directory(self, project_id: str) -> str:
        """Creates a directory for the given project_id and returns its path."""
        pass

    @abstractmethod
    def create_project_sub_directory(self, project_id: str, sub_folder: str) -> str:
        """Creates a sub-directory within a project's directory and returns its path."""
        pass

    @abstractmethod
    def get_file_size(self, file_path: str) -> int:
        """Returns the size of the file at the given path in bytes."""
        pass

    @abstractmethod
    def get_file_extension(self, file_name: str) -> str:
        """Returns the extension of the given file name (e.g., '.pdf')."""
        pass

    @abstractmethod
    def generate_unique_file_path(self, directory: str, original_file_name: str) -> str:
        """Generates a unique file path within the given directory for the original file name."""
        pass
