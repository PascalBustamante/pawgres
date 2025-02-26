from src.extract.petfinder import PetfinderAPI
import pandas as pd
from typing import List, Dict
import json


def process_dog_data(dogs_data: Dict) -> List[Dict]:
    """Process raw dog data into a cleaner format"""
    processed_dogs = []

    for dog in dogs_data["animals"]:
        processed_dog = {
            "id": dog.get("id"),
            "name": dog.get("name"),
            "breed_primary": dog.get("breeds", {}).get("primary"),
            "breed_secondary": dog.get("breeds", {}).get("secondary"),
            "age": dog.get("age"),
            "gender": dog.get("gender"),
            "size": dog.get("size"),
            "coat": dog.get("coat"),
            "status": dog.get("status"),
            "location": f"{dog.get('contact', {}).get('address', {}).get('city', '')}, "
            f"{dog.get('contact', {}).get('address', {}).get('state', '')}",
            "description": dog.get("description"),
            "photos": [photo.get("full") for photo in dog.get("photos", [])],
        }
        processed_dogs.append(processed_dog)

    return processed_dogs


def main():
    # Initialize the API
    try:
        api = PetfinderAPI()

        # Get some dogs
        dogs_data = api.get_dogs(location="New York, NY", distance=50, limit=100)
        print(dogs_data)

        # Process the data
        processed_dogs = process_dog_data(dogs_data)

        # Convert to DataFrame
        df = pd.DataFrame(processed_dogs)

        # Save to CSV
        df.to_csv("dogs_data.csv", index=False)

        # Save raw data for reference
        with open("raw_dogs_data.json", "w") as f:
            json.dump(dogs_data, f, indent=2)

        print(f"Retrieved {len(processed_dogs)} dogs")
        print(f"Data saved to 'dogs_data.csv' and 'raw_dogs_data.json'")

    except Exception as e:
        print(f"Error: {str(e)}")


from sqlalchemy import create_engine
import pandas as pd
from typing import Dict, List, Any
import os
from dotenv import load_dotenv
import psycopg

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


if __name__ == "__main__":
    load_dotenv()
    e = psycopg.connect(conninfo="postgresql://etl_pawgres_dev:axyzs@localhost:5432}/postgres}")
    # e = create_engine(
    #         "postgresql://etl_pawgres_dev:axyzs@localhost:5432}/postgres}"
    #     )
    print('connecting to database')
    # e.connect()
    print('connection established')