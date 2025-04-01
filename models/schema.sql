-- models/schema.sql

-- Table: weather_data
CREATE TABLE IF NOT EXISTS weather_data (
    city VARCHAR(100),
    weather_timestamp TIMESTAMP,
    lat FLOAT,
    lon FLOAT,
    temperature_celsius FLOAT,
    humidity INT,
    weather_description VARCHAR(255),
    PRIMARY KEY (city, weather_timestamp)
);

-- Table: shipments
CREATE TABLE IF NOT EXISTS shipments (
    id SERIAL PRIMARY KEY,
    truck VARCHAR(50),
    driver VARCHAR(50),
    shipment_start_timestamp TIMESTAMP,
    shipment_end_timestamp TIMESTAMP,
    start_location VARCHAR(100),
    end_location VARCHAR(100),
    shipment_distance FLOAT,
    consumed_fuel FLOAT
);

-- Example fact table for aggregated fuel consumption metrics.
CREATE TABLE IF NOT EXISTS fact_fuel_consumption (
    temp_range VARCHAR(20),
    avg_fuel_consumption FLOAT,
    PRIMARY KEY (temp_range)
);
