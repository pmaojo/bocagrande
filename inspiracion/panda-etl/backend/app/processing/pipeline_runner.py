from __future__ import annotations
import datetime
from typing import Dict, Any, Optional

import pandas as pd

from app.adapters.universal_io import (
    universal_extract_to_df,
    universal_write_df,
    UniversalIOError,
)
from app.logger import Logger

from .pipeline_repository import PipelineRepository


class PipelineExecutionError(Exception):
    pass


class PipelineRunner:
    """Execute pipelines defined in the repository."""

    def __init__(
        self, repository: PipelineRepository, logger: Logger | None = None
    ) -> None:
        self._repository = repository
        self._logger = logger or Logger()

    def run(
        self,
        pipeline_id: str,
        version: Optional[str] = None,
        input_config: Optional[Dict[str, Any]] = None,
        output_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            pipeline_version = self._repository.load_pipeline_version(
                pipeline_id, version
            )
            transform_script = pipeline_version.get("transform_script", "")
            nodes = pipeline_version.get("nodes", [])
            input_nodes = [n for n in nodes if n.get("type") == "input"]
            output_nodes = [n for n in nodes if n.get("type") == "output"]

            if input_config is None and input_nodes:
                input_config = input_nodes[0].get("data", {}).get("config", {})
            if output_config is None and output_nodes:
                output_config = output_nodes[0].get("data", {}).get("config", {})

            if not input_config:
                raise PipelineExecutionError("Input configuration missing")
            if not output_config:
                raise PipelineExecutionError("Output configuration missing")

            start_time = datetime.datetime.now()
            try:
                df_raw = universal_extract_to_df(input_config)
            except UniversalIOError as e:
                raise PipelineExecutionError(f"Error extracting data: {e}")

            try:
                local_vars = {"df_raw": df_raw}
                exec(transform_script, globals(), local_vars)
                if "df_transformed" not in local_vars:
                    raise PipelineExecutionError("df_transformed not produced")
                df_transformed = local_vars["df_transformed"]
                if not isinstance(df_transformed, pd.DataFrame):
                    raise PipelineExecutionError("df_transformed is not DataFrame")
            except Exception as e:  # pragma: no cover - executed code may fail
                raise PipelineExecutionError(f"Error executing transform: {e}")

            try:
                universal_write_df(df_transformed, output_config)
            except UniversalIOError as e:
                raise PipelineExecutionError(f"Error writing transformed data: {e}")

            end_time = datetime.datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            result = {
                "pipeline_id": pipeline_id,
                "version": pipeline_version.get("version_id"),
                "execution_time": execution_time,
                "rows_processed": len(df_raw),
                "rows_output": len(df_transformed),
                "columns_input": list(df_raw.columns),
                "columns_output": list(df_transformed.columns),
                "executed_at": end_time.isoformat(),
                "status": "success",
                "input_config": input_config,
                "output_config": output_config,
            }
            execution_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self._repository.save_execution_result(
                pipeline_id,
                pipeline_version.get("version_id") or version or "unknown",
                execution_id,
                result,
            )
            return result
        except Exception as exc:
            error_result = {
                "pipeline_id": pipeline_id,
                "version": version,
                "executed_at": datetime.datetime.now().isoformat(),
                "status": "error",
                "error_message": str(exc),
            }
            try:
                actual_version = version or self._repository.load_pipeline_version(
                    pipeline_id
                ).get("version_id")
                execution_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                self._repository.save_execution_result(
                    pipeline_id,
                    actual_version,
                    execution_id,
                    error_result,
                    is_error=True,
                )
            except Exception as log_exc:  # pragma: no cover - just log
                self._logger.error(f"Error saving execution error: {log_exc}")
            raise
