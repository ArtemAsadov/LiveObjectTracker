-- Initialize database schema
CREATE TABLE IF NOT EXISTS coordinates (
    object_id BIGINT PRIMARY KEY,
    x REAL,
    y REAL,
    z REAL,
    timestamp BIGINT
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_coordinates_timestamp ON coordinates(timestamp);