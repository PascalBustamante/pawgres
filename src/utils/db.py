from sqlalchemy import create_engine
import pandas as pd
from typing import Dict, List, Any
import os
from dotenv import load_dotenv

class DatabaseConnector:
    def __init__(self):
        load_dotenv()
        self.engine = self._create_engine()
        self.schemas = {
            'staging': 'staging_layer',
            'integration': 'integration_layer',
            'access': 'access_layer'
        }

    def _create_engine(self):
        return create_engine(
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
    
    def query_layer(self, layer: str, query: str) -> pd.DataFrame:
        """Query specific layer and returns a pandas df"""
        schema = self.schemas.get(layer)
        if not schema:
            return ValueError(f"Invalid layer: {layer}")
        
        full_query = f"""
            SELECT *
            FROM {schema}.{query}
        """

        return pd.read_sql(full_query, self.engine)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute custom query"""
        return pd.read_sql(query, self.engine)

    def get_table_info(self, layer: str, table: str) -> pd.DataFrame:
        """Get table information"""
        schema = self.schemas.get(layer)
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
        """
        return pd.read_sql(query, self.engine)