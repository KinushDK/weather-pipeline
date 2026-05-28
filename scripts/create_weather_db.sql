-- Create the database
CREATE DATABASE weather_db;

-- Connect to weather_db (run this separately in pgAdmin4 or use \c weather_db in psql)
-- \c weather_db;

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS raw;      -- Raw ingested data (as-is from source)
CREATE SCHEMA IF NOT EXISTS staging;  -- Cleaned/transformed intermediate tables
CREATE SCHEMA IF NOT EXISTS marts;    -- Final business-ready models (facts & dimensions)

-- ============================================
-- LAYER 1: RAW SCHEMA (Source data as-is)
-- ============================================

-- Raw table matching CSV structure exactly
CREATE TABLE raw.weather_events_raw (
    event_id VARCHAR(50),
    type VARCHAR(50),
    severity VARCHAR(20),
    start_time_utc VARCHAR(50),  -- Keep as string initially
    end_time_utc VARCHAR(50),    -- Keep as string initially
    precipitation_in VARCHAR(20), -- Keep as string initially
    timezone VARCHAR(50),
    airport_code VARCHAR(10),
    location_lat VARCHAR(20),
    location_lng VARCHAR(20),
    city VARCHAR(100),
    county VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
    batch_id VARCHAR(50)
);

-- ============================================
-- LAYER 2: STAGING SCHEMA (Cleaned & transformed)
-- ============================================

-- Staging table for weather events (cleaned, typed, validated)
CREATE TABLE staging.stg_weather_events (
    event_id VARCHAR(50) PRIMARY KEY,
    type VARCHAR(50),
    severity VARCHAR(20),
    start_time_utc VARCHAR(50),  -- Keep as text initially
    end_time_utc VARCHAR(50),    -- Keep as text initially
    precipitation_in DECIMAL(10,4),
    timezone VARCHAR(50),
    airport_code VARCHAR(10),
    location_lat DECIMAL(10,7),
    location_lng DECIMAL(10,7),
    city VARCHAR(100),
    county VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    duration_minutes INTEGER,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    data_quality_flag VARCHAR(20),
    staging_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging station information (deduplicated)
CREATE TABLE staging.stg_stations (
    airport_code VARCHAR(10) PRIMARY KEY,
    timezone VARCHAR(50),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    city VARCHAR(100),
    county VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    first_seen_date DATE,
    last_seen_date DATE,
    staging_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes on staging tables
CREATE INDEX idx_stg_weather_start_time ON staging.stg_weather_events(start_time_utc);
CREATE INDEX idx_stg_weather_type ON staging.stg_weather_events(type);
CREATE INDEX idx_stg_weather_state ON staging.stg_weather_events(state);
CREATE INDEX idx_stg_stations_state ON staging.stg_stations(state);

-- ============================================
-- LAYER 3: MARTS SCHEMA (Business-ready models)
-- ============================================

-- Dimension table for stations/airports
CREATE TABLE marts.dim_station (
    station_id SERIAL PRIMARY KEY,
    airport_code VARCHAR(10) UNIQUE NOT NULL,
    timezone VARCHAR(50),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    city VARCHAR(100),
    county VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    region VARCHAR(50),  -- Derived: Northeast, Southeast, etc.
    climate_zone VARCHAR(50), -- Derived if needed
    is_active BOOLEAN DEFAULT TRUE,
    valid_from DATE,
    valid_to DATE DEFAULT '9999-12-31',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension table for date (time dimension)
CREATE TABLE marts.dim_date (
    date_key INTEGER PRIMARY KEY,  -- YYYYMMDD format
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20),
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season VARCHAR(20)  -- Spring, Summer, Fall, Winter
);

-- Fact table for weather events (business metrics)
CREATE TABLE marts.fact_weather_events (
    event_id VARCHAR(50) PRIMARY KEY,
    station_id INTEGER REFERENCES marts.dim_station(station_id),
    date_key INTEGER REFERENCES marts.dim_date(date_key),
    type VARCHAR(50) NOT NULL,
    severity VARCHAR(20),
    start_time_utc TIMESTAMP NOT NULL,
    end_time_utc TIMESTAMP,
    precipitation_in DECIMAL(10,4),
    duration_minutes INTEGER,
    -- Derived metrics
    is_significant_event BOOLEAN,  -- Events lasting > 6 hours or high precipitation
    severity_score INTEGER,  -- 1=Light, 2=Moderate, 3=Severe, 4=Heavy
    precipitation_category VARCHAR(20), -- None, Light, Moderate, Heavy
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes on marts tables
CREATE INDEX idx_fact_events_start_time ON marts.fact_weather_events(start_time_utc);
CREATE INDEX idx_fact_events_type ON marts.fact_weather_events(type);
CREATE INDEX idx_fact_events_severity ON marts.fact_weather_events(severity);
CREATE INDEX idx_fact_events_station_id ON marts.fact_weather_events(station_id);
CREATE INDEX idx_fact_events_date_key ON marts.fact_weather_events(date_key);
CREATE INDEX idx_dim_station_state ON marts.dim_station(state);
CREATE INDEX idx_dim_station_airport ON marts.dim_station(airport_code);

-- ============================================
-- CREATE VIEWS FOR EASY ACCESS
-- ============================================

-- View that shows the complete data flow
CREATE OR REPLACE VIEW marts.v_weather_events_complete AS
SELECT 
    f.event_id,
    f.type,
    f.severity,
    f.start_time_utc,
    f.end_time_utc,
    f.precipitation_in,
    f.duration_minutes,
    f.is_significant_event,
    s.airport_code,
    s.city,
    s.county,
    s.state,
    s.region,
    d.year,
    d.month,
    d.month_name,
    d.season,
    d.day_of_week,
    d.is_weekend
FROM marts.fact_weather_events f
LEFT JOIN marts.dim_station s ON f.station_id = s.station_id
LEFT JOIN marts.dim_date d ON f.date_key = d.date_key;

-- View for monthly aggregations
CREATE OR REPLACE VIEW marts.v_monthly_weather_summary AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    f.type,
    COUNT(*) as event_count,
    AVG(f.precipitation_in) as avg_precipitation,
    SUM(f.precipitation_in) as total_precipitation,
    AVG(f.duration_minutes) as avg_duration_minutes,
    COUNT(CASE WHEN f.is_significant_event THEN 1 END) as significant_events_count
FROM marts.fact_weather_events f
JOIN marts.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, f.type
ORDER BY d.year, d.month, f.type;

-- Grant permissions
GRANT ALL ON SCHEMA raw TO postgres;
GRANT ALL ON SCHEMA staging TO postgres;
GRANT ALL ON SCHEMA marts TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA raw TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA staging TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA marts TO postgres;