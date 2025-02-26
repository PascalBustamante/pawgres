# database_setup.py
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database_and_tables():
    # Connection parameters for initial connection
    initial_params = {
        "host": "localhost",
        "user": "postgres",  # your default superuser
        "password": "your_password",
        "port": "5432",
    }

    db_name = "petfinder_db"

    # Create database
    try:
        # Connect to PostgreSQL server
        conn = psycopg2.connect(**initial_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Create database if it doesn't exist
        cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {db_name}")
            print(f"Database '{db_name}' created successfully")

        cur.close()
        conn.close()

        # Connect to the new database
        db_params = {**initial_params, "database": db_name}
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Create tables
        create_tables = """
        -- Raw data table
        CREATE TABLE IF NOT EXISTS raw_dogs (
            id SERIAL PRIMARY KEY,
            data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Processed dogs table
        CREATE TABLE IF NOT EXISTS dogs (
            id SERIAL PRIMARY KEY,
            petfinder_id INTEGER UNIQUE,
            name VARCHAR(255),
            age VARCHAR(50),
            gender VARCHAR(50),
            size VARCHAR(50),
            breed_primary VARCHAR(255),
            breed_secondary VARCHAR(255),
            is_mixed_breed BOOLEAN,
            status VARCHAR(50),
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Location table
        CREATE TABLE IF NOT EXISTS locations (
            id SERIAL PRIMARY KEY,
            dog_id INTEGER REFERENCES dogs(id),
            city VARCHAR(255),
            state VARCHAR(100),
            postcode VARCHAR(20),
            country VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Photos table
        CREATE TABLE IF NOT EXISTS photos (
            id SERIAL PRIMARY KEY,
            dog_id INTEGER REFERENCES dogs(id),
            small_url TEXT,
            medium_url TEXT,
            large_url TEXT,
            full_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_dogs_status ON dogs(status);
        CREATE INDEX IF NOT EXISTS idx_dogs_breed ON dogs(breed_primary);
        CREATE INDEX IF NOT EXISTS idx_locations_state ON locations(state);
        """

        # Execute create tables commands
        cur.execute(create_tables)
        conn.commit()
        print("Tables created successfully")

        # Create views for analytics
        create_views = """
        -- Breed statistics view
        CREATE MATERIALIZED VIEW IF NOT EXISTS breed_statistics AS
        SELECT 
            breed_primary,
            COUNT(*) as total_dogs,
            COUNT(*) FILTER (WHERE status = 'adoptable') as available_dogs,
            COUNT(*) FILTER (WHERE is_mixed_breed) as mixed_breed_count,
            ROUND(AVG(LENGTH(description))::numeric, 2) as avg_description_length
        FROM dogs
        GROUP BY breed_primary;

        -- Location statistics view
        CREATE MATERIALIZED VIEW IF NOT EXISTS location_statistics AS
        SELECT 
            l.state,
            COUNT(*) as total_dogs,
            COUNT(*) FILTER (WHERE d.status = 'adopted') as adopted_count,
            ROUND(COUNT(*) FILTER (WHERE d.status = 'adopted')::numeric / 
                  COUNT(*)::numeric * 100, 2) as adoption_rate
        FROM dogs d
        JOIN locations l ON d.id = l.dog_id
        GROUP BY l.state;
        """

        cur.execute(create_views)
        conn.commit()
        print("Views created successfully")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    create_database_and_tables()
