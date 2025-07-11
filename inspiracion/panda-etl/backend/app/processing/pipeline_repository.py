from __future__ import annotations
import datetime
import uuid
from typing import Dict, Any, List, Optional

from app.logger import Logger
from app.interfaces.pipeline_definition_store_port import PipelineDefinitionStorePort


class PipelineRepository:
    """Handle persistence of pipeline definitions using the configured store."""

    def __init__(
        self, store: PipelineDefinitionStorePort, logger: Logger | None = None
    ) -> None:
        self._store = store
        self._logger = logger or Logger()

    def create_pipeline(
        self, name: str, description: str = "", project_id: Optional[int] = None
    ) -> str:
        pipeline_id = str(uuid.uuid4())
        self._store.ensure_pipeline_directories_exist(pipeline_id)
        metadata = {
            "id": pipeline_id,
            "name": name,
            "description": description,
            "project_id": project_id,
            "created_at": datetime.datetime.now().isoformat(),
            "updated_at": datetime.datetime.now().isoformat(),
            "version_count": 0,
        }
        self._store.write_metadata(pipeline_id, metadata)
        self.save_pipeline_version(
            pipeline_id=pipeline_id,
            nodes=[],
            edges=[],
            transform_script=(
                "# Código de transformación\n\n"
                "# El DataFrame de entrada está disponible como 'df_raw'\n"
                "# El DataFrame transformado debe asignarse a 'df_transformed'\n\n"
                "df_transformed = df_raw.copy()"
            ),
            config={},
            version_name="initial",
        )
        return pipeline_id

    def save_pipeline_version(
        self,
        pipeline_id: str,
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
        transform_script: str,
        config: Dict[str, Any],
        version_name: Optional[str] = None,
        description: str = "",
    ) -> str:
        metadata = self._store.read_metadata(pipeline_id)
        metadata["version_count"] = metadata.get("version_count", 0) + 1
        metadata["updated_at"] = datetime.datetime.now().isoformat()
        version_id = version_name or datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        structure_data = {"nodes": nodes, "edges": edges}
        self._store.write_version_structure(pipeline_id, version_id, structure_data)
        self._store.write_version_transform_script(
            pipeline_id, version_id, transform_script
        )
        version_metadata = {
            "version_id": version_id,
            "pipeline_id": pipeline_id,
            "created_at": datetime.datetime.now().isoformat(),
            "description": description,
            "config": config,
        }
        self._store.write_version_metadata(pipeline_id, version_id, version_metadata)
        self._store.write_metadata(pipeline_id, metadata)
        return version_id

    def load_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        metadata = self._store.read_metadata(pipeline_id)
        versions = self._store.list_versions_for_pipeline(pipeline_id)
        metadata["versions"] = versions
        return metadata

    def load_pipeline_version(
        self, pipeline_id: str, version: Optional[str] = None
    ) -> Dict[str, Any]:
        actual_version = version or self._store.get_latest_version_id(pipeline_id)
        if not actual_version:
            raise FileNotFoundError(f"No versions found for pipeline {pipeline_id}")
        structure = self._store.read_version_structure(pipeline_id, actual_version)
        script = self._store.read_version_transform_script(pipeline_id, actual_version)
        version_metadata = self._store.read_version_metadata(
            pipeline_id, actual_version
        )
        return {
            **version_metadata,
            "nodes": structure.get("nodes", []),
            "edges": structure.get("edges", []),
            "transform_script": script,
        }

    def list_pipelines(self, project_id: Optional[int] = None) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for pid in self._store.list_all_pipeline_ids():
            try:
                meta = self._store.read_metadata(pid)
                if project_id is not None and meta.get("project_id") != project_id:
                    continue
                result.append(meta)
            except Exception as exc:  # pragma: no cover - log but continue
                self._logger.error(f"Error reading metadata for pipeline {pid}: {exc}")
        result.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
        return result

    def list_pipeline_versions(self, pipeline_id: str) -> List[Dict[str, Any]]:
        self._store.read_metadata(pipeline_id)  # raises if not found
        versions = []
        for vid in self._store.list_versions_for_pipeline(pipeline_id):
            try:
                versions.append(self._store.read_version_metadata(pipeline_id, vid))
            except Exception as exc:  # pragma: no cover - log but continue
                self._logger.error(f"Error reading metadata for version {vid}: {exc}")
        return versions

    def delete_pipeline(self, pipeline_id: str) -> None:
        self._store.delete_pipeline(pipeline_id)

    def delete_pipeline_version(self, pipeline_id: str, version: str) -> None:
        versions = self._store.list_versions_for_pipeline(pipeline_id)
        if len(versions) <= 1 and version in versions:
            raise ValueError("Cannot delete the only pipeline version")
        self._store.delete_version(pipeline_id, version)
        metadata = self._store.read_metadata(pipeline_id)
        metadata["version_count"] = max(0, metadata.get("version_count", 1) - 1)
        metadata["updated_at"] = datetime.datetime.now().isoformat()
        self._store.write_metadata(pipeline_id, metadata)

    def save_execution_result(
        self,
        pipeline_id: str,
        version: str,
        execution_id: str,
        result_data: Dict[str, Any],
        is_error: bool = False,
    ) -> None:
        self._store.save_execution_result(
            pipeline_id, version, execution_id, result_data, is_error
        )
