from sqlalchemy import create_engine
from config.config import config
import pandas as pd
from typing import Dict, List, Any
import os

class DatabaseConnector:
    def __init__(self, filename="config/config.ini", section="postgresql"):
        self.credentials = config(filename=filename, section=section)
        self.engine = self._create_engine()
        self.schemas = {
            'staging': 'staging',
            'intermediate': 'intermediate',
            'marts': 'marts',
            'logging': 'logging'
        }

    def _create_engine(self):
        """Create a database engine"""
        url = "postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
            self.credentials["user"], 
            self.credentials["password"], 
            self.credentials["host"], 
            self.credentials["port"], 
            self.credentials["dbname"]
            )
        return create_engine(url=url)
    
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