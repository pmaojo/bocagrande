from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple

class PipelineDefinitionStorePort(ABC):
    @abstractmethod
    def get_pipeline_directory_path(self, pipeline_id: str) -> str:
        """Returns the absolute path to the directory for a given pipeline_id."""
        pass

    @abstractmethod
    def get_pipeline_version_directory_path(self, pipeline_id: str, version: str) -> str:
        """Returns the absolute path to a specific version directory of a pipeline."""
        pass

    @abstractmethod
    def get_latest_version_id(self, pipeline_id: str) -> Optional[str]:
        """
        Determines and returns the ID of the latest version for a given pipeline.
        Returns None if no versions exist.
        """
        pass

    @abstractmethod
    def ensure_pipeline_directories_exist(self, pipeline_id: str) -> Tuple[str, str]:
        """
        Ensures that the main pipeline directory and its 'versions' subdirectory exist.
        Returns a tuple of (pipeline_dir_path, versions_subdir_path).
        """
        pass

    @abstractmethod
    def read_metadata(self, pipeline_id: str) -> Dict[str, Any]:
        """Reads the main metadata file for a pipeline."""
        pass

    @abstractmethod
    def write_metadata(self, pipeline_id: str, metadata: Dict[str, Any]) -> None:
        """Writes the main metadata file for a pipeline."""
        pass

    @abstractmethod
    def read_version_structure(self, pipeline_id: str, version: str) -> Dict[str, Any]:
        """Reads the structure.yaml file for a specific pipeline version."""
        pass

    @abstractmethod
    def write_version_structure(self, pipeline_id: str, version: str, structure: Dict[str, Any]) -> None:
        """Writes the structure.yaml file for a specific pipeline version."""
        pass

    @abstractmethod
    def read_version_transform_script(self, pipeline_id: str, version: str) -> str:
        """Reads the transform.py file for a specific pipeline version."""
        pass

    @abstractmethod
    def write_version_transform_script(self, pipeline_id: str, version: str, script_content: str) -> None:
        """Writes the transform.py file for a specific pipeline version."""
        pass

    @abstractmethod
    def read_version_metadata(self, pipeline_id: str, version: str) -> Dict[str, Any]:
        """Reads the metadata.json file for a specific pipeline version."""
        pass

    @abstractmethod
    def write_version_metadata(self, pipeline_id: str, version: str, version_metadata: Dict[str, Any]) -> None:
        """Writes the metadata.json file for a specific pipeline version."""
        pass

    @abstractmethod
    def list_all_pipeline_ids(self) -> List[str]:
        """Lists the IDs of all pipelines present in the store."""
        pass

    @abstractmethod
    def list_versions_for_pipeline(self, pipeline_id: str) -> List[str]:
        """Lists all version IDs for a given pipeline, typically sorted latest first."""
        pass

    @abstractmethod
    def delete_pipeline(self, pipeline_id: str) -> None:
        """Deletes an entire pipeline, including all its versions and metadata."""
        pass

    @abstractmethod
    def delete_version(self, pipeline_id: str, version: str) -> None:
        """Deletes a specific version of a pipeline."""
        pass

    @abstractmethod
    def save_execution_result(self, pipeline_id: str, version: str, execution_id: str,
                              result_data: Dict[str, Any], is_error: bool = False) -> None:
        """Saves the result (or error) of a pipeline execution to the specific version's execution history."""
        pass

# Added Tuple to typing imports for ensure_pipeline_directories_exist
# from typing import Tuple # Moved to top
