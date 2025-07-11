from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple
import pandas as pd

class LLMServicePort(ABC):
    @abstractmethod
    def analyze_columns_for_script(self, raw_df: pd.DataFrame, target_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Analyzes columns from raw and target dataframes to assist in script generation.
        Returns a DataFrame (e.g., sample data) and a dictionary of analysis results.
        """
        pass

    @abstractmethod
    def generate_transformation_script(self,
                                       source_data: Dict[str, Any],
                                       target_schema: Optional[Dict[str, str]] = None,
                                       transformation_description: str = "",
                                       node_id: str = "",
                                       node_name: str = "") -> Dict[str, Any]:
        """
        Generates a transformation script based on source data, target schema, and description.
        Returns a dictionary containing the generated script and other relevant information.
        """
        pass

    @abstractmethod
    def analyze_etl_flow(self, flow_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyzes an ETL flow described by flow_data.
        Returns a dictionary containing analysis or suggestions.
        """
        pass
