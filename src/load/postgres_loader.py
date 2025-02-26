import psycopg2
from psycopg2.extras import Json
from config.database import DB_CONFIG


class PostgresLoader:
    def __init__(self):
        self.db_params = DB_CONFIG

    def bulk_load_raw_data(self, data):
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO raw_dogs (data) VALUES %s",
                    [(Json(record),) for record in data],
                )
