CREATE TABLE IF NOT EXISTS raw_dogs (
    id SERIAL PRIMARY KEY,
    data JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_dogs (
    id SERIAL PRIMARY KEY,
    dog_id INTEGER,
    name VARCHAR(255),
    breed VARCHAR(255),
    -- other fields
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);