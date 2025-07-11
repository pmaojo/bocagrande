import pandas as pd
from unittest.mock import MagicMock, patch

from app.processing.pipeline_runner import PipelineRunner
from app.processing.pipeline_repository import PipelineRepository
from app.logger import Logger


def test_run_pipeline_basic():
    repo = MagicMock(spec=PipelineRepository)
    repo.load_pipeline_version.return_value = {
        "version_id": "v1",
        "nodes": [
            {"id": "1", "type": "input", "data": {"config": {"path": "in.csv"}}},
            {"id": "2", "type": "output", "data": {"config": {"path": "out.csv"}}},
        ],
        "edges": [],
        "transform_script": "df_transformed = df_raw",
    }

    runner = PipelineRunner(repository=repo, logger=Logger())

    with patch(
        "app.processing.pipeline_runner.universal_extract_to_df"
    ) as m_ext, patch("app.processing.pipeline_runner.universal_write_df") as m_write:
        m_ext.return_value = pd.DataFrame({"a": [1, 2]})
        m_write.return_value = None

        result = runner.run("p1")

        m_ext.assert_called_once()
        m_write.assert_called_once()
        assert result["status"] == "success"
        assert result["rows_processed"] == 2
