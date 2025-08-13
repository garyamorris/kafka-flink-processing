-- Create the events table
CREATE TABLE IF NOT EXISTS events (
    id INT PRIMARY KEY,
    ts TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

-- Create an index on timestamp for better query performance
CREATE INDEX idx_events_ts ON events(ts);

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE events TO postgres;