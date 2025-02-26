from src.extract.petfinder import PetfinderAPI 
import pandas as pd
from typing import List, Dict
import json

def process_dog_data(dogs_data: Dict) -> List[Dict]:
    """Process raw dog data into a cleaner format"""
    processed_dogs = []
    
    for dog in dogs_data['animals']:
        processed_dog = {
            'id': dog.get('id'),
            'name': dog.get('name'),
            'breed_primary': dog.get('breeds', {}).get('primary'),
            'breed_secondary': dog.get('breeds', {}).get('secondary'),
            'age': dog.get('age'),
            'gender': dog.get('gender'),
            'size': dog.get('size'),
            'coat': dog.get('coat'),
            'status': dog.get('status'),
            'location': f"{dog.get('contact', {}).get('address', {}).get('city', '')}, "
                       f"{dog.get('contact', {}).get('address', {}).get('state', '')}",
            'description': dog.get('description'),
            'photos': [photo.get('full') for photo in dog.get('photos', [])]
        }
        processed_dogs.append(processed_dog)
    
    return processed_dogs

def main():
    # Initialize the API
    try:
        api = PetfinderAPI()
        
        # Get some dogs
        dogs_data = api.get_dogs(
            location="New York, NY",
            distance=50,
            limit=100
        )
        print(dogs_data)
        
        # Process the data
        processed_dogs = process_dog_data(dogs_data)
        
        # Convert to DataFrame
        df = pd.DataFrame(processed_dogs)
        
        # Save to CSV
        df.to_csv('dogs_data.csv', index=False)
        
        # Save raw data for reference
        with open('raw_dogs_data.json', 'w') as f:
            json.dump(dogs_data, f, indent=2)
            
        print(f"Retrieved {len(processed_dogs)} dogs")
        print(f"Data saved to 'dogs_data.csv' and 'raw_dogs_data.json'")
        
    except Exception as e:
        print(f"Error: {str(e)}")

from config.config import config
import psycopg

def connect():
    connection = None
    try:
        params = config()
        print(params)
        print("Connecting to db")
        conninfo = "postgresql://postgres:11.Axzys@localhost:5432/postgres"
        #connection = psycopg.connect()
        # Connect to database
        with psycopg.connect(conninfo=conninfo	
        ) as conn:
    
            # Create cursor and execute query
            with conn.cursor() as cur:
                cur.execute('SELECT version()')
                version = cur.fetchone()
                print(f"Connected! PostgreSQL version: {version}")
                cur.close()

    except(Exception, psycopg.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if connection is not None:
            connection.close()
            print("Database connection closed")

if __name__ == "__main__":
    conninfo = "postgres://postgres:11.Axzys@localhost:5432/postgres"

    psycopg.connect(conninfo=conninfo)