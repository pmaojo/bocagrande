import os
import yaml
import json
import shutil
from typing import Dict, Any, List, Optional, Tuple # Moved Tuple here
from app.interfaces.pipeline_definition_store_port import PipelineDefinitionStorePort
from app.logger import get_logger # Assuming a logger might be useful

logger = get_logger(__name__)

class FileSystemPipelineDefinitionStoreAdapter(PipelineDefinitionStorePort):
    PIPELINE_METADATA_FILE = "metadata.json"
    VERSIONS_SUBDIR = "versions"
    STRUCTURE_FILE = "structure.yaml"
    TRANSFORM_SCRIPT_FILE = "transform.py"
    VERSION_METADATA_FILE = "metadata.json" # Same name as pipeline metadata, but in version dir
    EXECUTIONS_SUBDIR = "executions"

    def __init__(self, base_pipelines_dir: str):
        self.base_dir = base_pipelines_dir
        os.makedirs(self.base_dir, exist_ok=True)
        logger.info(f"FileSystemPipelineDefinitionStoreAdapter initialized with base directory: {self.base_dir}")

    def get_pipeline_directory_path(self, pipeline_id: str) -> str:
        return os.path.join(self.base_dir, pipeline_id)

    def get_pipeline_version_directory_path(self, pipeline_id: str, version: str) -> str:
        return os.path.join(self.get_pipeline_directory_path(pipeline_id), self.VERSIONS_SUBDIR, version)

    def get_latest_version_id(self, pipeline_id: str) -> Optional[str]:
        pipeline_dir = self.get_pipeline_directory_path(pipeline_id)
        versions_dir = os.path.join(pipeline_dir, self.VERSIONS_SUBDIR)
        if not os.path.exists(versions_dir) or not os.path.isdir(versions_dir):
            return None

        versions = [d for d in os.listdir(versions_dir) if os.path.isdir(os.path.join(versions_dir, d))]
        if not versions:
            return None

        # Assuming versions are sortable (e.g., timestamps or semantic versioning)
        # For timestamp-based like "20230101_120000", reverse sort works.
        # If using semantic versioning, a more complex sort key would be needed.
        versions.sort(reverse=True)
        return versions[0]

    def ensure_pipeline_directories_exist(self, pipeline_id: str) -> Tuple[str, str]:
        pipeline_dir = self.get_pipeline_directory_path(pipeline_id)
        versions_subdir = os.path.join(pipeline_dir, self.VERSIONS_SUBDIR)
        os.makedirs(pipeline_dir, exist_ok=True)
        os.makedirs(versions_subdir, exist_ok=True)
        return pipeline_dir, versions_subdir

    def _read_json_file(self, file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"File not found: {file_path}")
            raise # Or return None / empty dict based on desired strictness
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {file_path}: {e}")
            raise # Or handle appropriately

    def _write_json_file(self, file_path: str, data: Dict[str, Any]) -> None:
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except IOError as e:
            logger.error(f"Error writing JSON to {file_path}: {e}")
            raise

    def _read_yaml_file(self, file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"File not found: {file_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error decoding YAML from {file_path}: {e}")
            raise

    def _write_yaml_file(self, file_path: str, data: Dict[str, Any]) -> None:
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.dump(data, f, default_flow_style=False)
        except IOError as e:
            logger.error(f"Error writing YAML to {file_path}: {e}")
            raise

    def _read_text_file(self, file_path: str) -> str:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            logger.warning(f"File not found: {file_path}")
            raise

    def _write_text_file(self, file_path: str, content: str) -> None:
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
        except IOError as e:
            logger.error(f"Error writing text to {file_path}: {e}")
            raise

    def read_metadata(self, pipeline_id: str) -> Dict[str, Any]:
        file_path = os.path.join(self.get_pipeline_directory_path(pipeline_id), self.PIPELINE_METADATA_FILE)
        return self._read_json_file(file_path)

    def write_metadata(self, pipeline_id: str, metadata: Dict[str, Any]) -> None:
        file_path = os.path.join(self.get_pipeline_directory_path(pipeline_id), self.PIPELINE_METADATA_FILE)
        self._write_json_file(file_path, metadata)

    def read_version_structure(self, pipeline_id: str, version: str) -> Dict[str, Any]:
        file_path = os.path.join(self.get_pipeline_version_directory_path(pipeline_id, version), self.STRUCTURE_FILE)
        return self._read_yaml_file(file_path)

    def write_version_structure(self, pipeline_id: str, version: str, structure: Dict[str, Any]) -> None:
        file_path = os.path.join(self.get_pipeline_version_directory_path(pipeline_id, version), self.STRUCTURE_FILE)
        self._write_yaml_file(file_path, structure)

    def read_version_transform_script(self, pipeline_id: str, version: str) -> str:
        file_path = os.path.join(self.get_pipeline_version_directory_path(pipeline_id, version), self.TRANSFORM_SCRIPT_FILE)
        return self._read_text_file(file_path)

    def write_version_transform_script(self, pipeline_id: str, version: str, script_content: str) -> None:
        file_path = os.path.join(self.get_pipeline_version_directory_path(pipeline_id, version), self.TRANSFORM_SCRIPT_FILE)
        self._write_text_file(file_path, script_content)

    def read_version_metadata(self, pipeline_id: str, version: str) -> Dict[str, Any]:
        file_path = os.path.join(self.get_pipeline_version_directory_path(pipeline_id, version), self.VERSION_METADATA_FILE)
        return self._read_json_file(file_path)

    def write_version_metadata(self, pipeline_id: str, version: str, version_metadata: Dict[str, Any]) -> None:
        file_path = os.path.join(self.get_pipeline_version_directory_path(pipeline_id, version), self.VERSION_METADATA_FILE)
        self._write_json_file(file_path, version_metadata)

    def list_all_pipeline_ids(self) -> List[str]:
        ids = []
        if os.path.exists(self.base_dir) and os.path.isdir(self.base_dir):
            for item_name in os.listdir(self.base_dir):
                item_path = os.path.join(self.base_dir, item_name)
                # Check if it's a directory and potentially a pipeline (e.g., contains metadata.json)
                if os.path.isdir(item_path) and \
                   os.path.exists(os.path.join(item_path, self.PIPELINE_METADATA_FILE)):
                    ids.append(item_name)
        return ids

    def list_versions_for_pipeline(self, pipeline_id: str) -> List[str]:
        versions_dir = os.path.join(self.get_pipeline_directory_path(pipeline_id), self.VERSIONS_SUBDIR)
        versions = []
        if os.path.exists(versions_dir) and os.path.isdir(versions_dir):
            for version_id in os.listdir(versions_dir):
                if os.path.isdir(os.path.join(versions_dir, version_id)):
                    versions.append(version_id)
        versions.sort(reverse=True) # Assuming sortable IDs, latest first
        return versions

    def delete_pipeline(self, pipeline_id: str) -> None:
        pipeline_dir = self.get_pipeline_directory_path(pipeline_id)
        try:
            if os.path.exists(pipeline_dir) and os.path.isdir(pipeline_dir):
                shutil.rmtree(pipeline_dir)
                logger.info(f"Deleted pipeline directory: {pipeline_dir}")
            else:
                logger.warning(f"Pipeline directory not found for deletion: {pipeline_dir}")
        except Exception as e:
            logger.error(f"Error deleting pipeline directory {pipeline_dir}: {e}")
            raise

    def delete_version(self, pipeline_id: str, version: str) -> None:
        version_dir = self.get_pipeline_version_directory_path(pipeline_id, version)
        try:
            if os.path.exists(version_dir) and os.path.isdir(version_dir):
                shutil.rmtree(version_dir)
                logger.info(f"Deleted pipeline version directory: {version_dir}")
            else:
                logger.warning(f"Pipeline version directory not found for deletion: {version_dir}")
        except Exception as e:
            logger.error(f"Error deleting pipeline version directory {version_dir}: {e}")
            raise

    def save_execution_result(self, pipeline_id: str, version: str, execution_id: str,
                              result_data: Dict[str, Any], is_error: bool = False) -> None:
        version_dir = self.get_pipeline_version_directory_path(pipeline_id, version)
        executions_dir = os.path.join(version_dir, self.EXECUTIONS_SUBDIR)
        os.makedirs(executions_dir, exist_ok=True)

        file_suffix = "_error" if is_error else ""
        result_file_name = f"{execution_id}{file_suffix}.json"
        file_path = os.path.join(executions_dir, result_file_name)

        self._write_json_file(file_path, result_data)
        logger.info(f"Saved execution result to {file_path}")
