import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CSV_PATH = r'D:\Personnal\weather_pipeline\WeatherEvents_Jan2016-Dec2022.csv'
DB_CONFIG = {
    'host': 'localhost',
    'database': 'weather_db',
    'user': 'postgres',
    'password': 'sekonda',  # Change this
    'port': 5432
}

def create_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(**DB_CONFIG)

def load_raw_data(conn, csv_path):
    """Load raw CSV data into raw schema"""
    logger.info("Loading raw data into RAW schema...")
    cursor = conn.cursor()
    
    # Read CSV with all columns as strings initially
    df = pd.read_csv(csv_path, encoding='latin1', dtype=str)
    
    # Generate batch ID
    batch_id = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
    
    # Prepare data for insertion
    values = []
    for _, row in df.iterrows():
        values.append((
            row.get('EventId', ''),
            row.get('Type', ''),
            row.get('Severity', ''),
            row.get('StartTime(UTC)', ''),
            row.get('EndTime(UTC)', ''),
            row.get('Precipitation(in)', ''),
            row.get('TimeZone', ''),
            row.get('AirportCode', ''),
            row.get('LocationLat', ''),
            row.get('LocationLng', ''),
            row.get('City', ''),
            row.get('County', ''),
            row.get('State', ''),
            row.get('ZipCode', ''),
            batch_id
        ))
    
    # Truncate raw table before loading (for demo purposes)
    cursor.execute("TRUNCATE TABLE raw.weather_events_raw;")
    
    # Insert into raw schema
    insert_query = """
    INSERT INTO raw.weather_events_raw 
    (event_id, type, severity, start_time_utc, end_time_utc, precipitation_in,
     timezone, airport_code, location_lat, location_lng, city, county, state, 
     zip_code, batch_id)
    VALUES %s
    """
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    
    logger.info(f"Loaded {len(values)} rows into raw.weather_events_raw (batch_id: {batch_id})")
    return batch_id

def transform_raw_to_staging(conn, batch_id):
    """Transform data from raw schema to staging schema"""
    logger.info("Transforming data from RAW to STAGING schema...")
    cursor = conn.cursor()
    
    # First, let's see what date formats we have
    cursor.execute("""
        SELECT DISTINCT start_time_utc 
        FROM raw.weather_events_raw 
        WHERE batch_id = %s 
        LIMIT 5
    """, (batch_id,))
    sample_dates = cursor.fetchall()
    logger.info(f"Sample dates in raw data: {sample_dates}")
    
    # SQL to transform and move data from raw to staging
    # FIXED: Handle dates that are already in correct format
    transform_sql = """
    INSERT INTO staging.stg_weather_events 
    (event_id, type, severity, start_time_utc, end_time_utc, 
     precipitation_in, timezone, airport_code, location_lat, location_lng,
     city, county, state, zip_code, duration_minutes, year, month, day, hour,
     data_quality_flag)
    SELECT 
        event_id,
        type,
        CASE 
            WHEN severity IN ('Light', 'Moderate', 'Severe', 'Heavy') THEN severity
            ELSE 'Unknown'
        END as severity,
        -- FIXED: Handle dates that are already in correct format
        CASE 
            -- If already in YYYY-MM-DD HH:MI:SS format, cast directly
            WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                THEN start_time_utc::TIMESTAMP
            -- If in MM/DD/YYYY format, convert
            WHEN start_time_utc ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}$' 
                THEN TO_TIMESTAMP(start_time_utc, 'YYYY-MM-DD HH24:MI:SS')
            ELSE NULL
        END as start_time_utc,
        CASE 
            WHEN end_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                THEN end_time_utc::TIMESTAMP
            WHEN end_time_utc ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}$' 
                THEN TO_TIMESTAMP(end_time_utc, 'YYYY-MM-DD HH24:MI:SS')
            ELSE NULL
        END as end_time_utc,
        -- Convert precipitation to numeric
        CASE 
            WHEN precipitation_in ~ '^[0-9.]+$' THEN precipitation_in::DECIMAL(10,4)
            ELSE 0
        END as precipitation_in,
        timezone,
        COALESCE(airport_code, 'UNKNOWN') as airport_code,
        CASE WHEN location_lat ~ '^[0-9.-]+$' THEN location_lat::DECIMAL(10,7) ELSE NULL END,
        CASE WHEN location_lng ~ '^[0-9.-]+$' THEN location_lng::DECIMAL(10,7) ELSE NULL END,
        COALESCE(city, 'Unknown') as city,
        COALESCE(county, 'Unknown') as county,
        COALESCE(state, 'Unknown') as state,
        COALESCE(zip_code, '') as zip_code,
        -- Calculate duration in minutes
        EXTRACT(EPOCH FROM (
            (CASE 
                WHEN end_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                    THEN end_time_utc::TIMESTAMP
                WHEN end_time_utc ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}$' 
                    THEN TO_TIMESTAMP(end_time_utc, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END) - 
            (CASE 
                WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                    THEN start_time_utc::TIMESTAMP
                WHEN start_time_utc ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}$' 
                    THEN TO_TIMESTAMP(start_time_utc, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END)
        ))/60 as duration_minutes,
        EXTRACT(YEAR FROM 
            CASE 
                WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                    THEN start_time_utc::TIMESTAMP
                WHEN start_time_utc ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}$' 
                    THEN TO_TIMESTAMP(start_time_utc, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END
        ) as year,
        EXTRACT(MONTH FROM 
            CASE 
                WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                    THEN start_time_utc::TIMESTAMP
                WHEN start_time_utc ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}$' 
                    THEN TO_TIMESTAMP(start_time_utc, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END
        ) as month,
        EXTRACT(DAY FROM 
            CASE 
                WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                    THEN start_time_utc::TIMESTAMP
                WHEN start_time_utc ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}$' 
                    THEN TO_TIMESTAMP(start_time_utc, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END
        ) as day,
        EXTRACT(HOUR FROM 
            CASE 
                WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' 
                    THEN start_time_utc::TIMESTAMP
                WHEN start_time_utc ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}$' 
                    THEN TO_TIMESTAMP(start_time_utc, 'YYYY-MM-DD HH24:MI:SS')
                ELSE NULL
            END
        ) as hour,
        -- Data quality flag
        CASE 
            WHEN start_time_utc IS NULL OR end_time_utc IS NULL THEN 'INVALID_DATE'
            WHEN start_time_utc !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' AND start_time_utc !~ '^[0-9]{1,2}/' THEN 'INVALID_DATE_FORMAT'
            WHEN severity NOT IN ('Light', 'Moderate', 'Severe', 'Heavy') THEN 'INVALID_SEVERITY'
            ELSE 'VALID'
        END as data_quality_flag
    FROM raw.weather_events_raw
    WHERE batch_id = %s
    ON CONFLICT (event_id) DO UPDATE SET
        type = EXCLUDED.type,
        severity = EXCLUDED.severity,
        data_quality_flag = EXCLUDED.data_quality_flag;
    """
    
    cursor.execute(transform_sql, (batch_id,))
    conn.commit()
    
    # Also populate staging stations table
    stations_sql = """
    INSERT INTO staging.stg_stations 
    (airport_code, timezone, latitude, longitude, city, county, state, zip_code,
     first_seen_date, last_seen_date)
    SELECT 
        airport_code,
        MAX(timezone) as timezone,
        MAX(location_lat) as latitude,
        MAX(location_lng) as longitude,
        MAX(city) as city,
        MAX(county) as county,
        MAX(state) as state,
        MAX(zip_code) as zip_code,
        MIN(start_time_utc::DATE) as first_seen_date,
        MAX(start_time_utc::DATE) as last_seen_date
    FROM staging.stg_weather_events
    WHERE airport_code != 'UNKNOWN'
    GROUP BY airport_code
    ON CONFLICT (airport_code) DO UPDATE SET
        timezone = EXCLUDED.timezone,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        city = EXCLUDED.city,
        county = EXCLUDED.county,
        state = EXCLUDED.state,
        zip_code = EXCLUDED.zip_code,
        last_seen_date = EXCLUDED.last_seen_date,
        staging_loaded_at = CURRENT_TIMESTAMP;
    """
    
    cursor.execute(stations_sql)
    conn.commit()
    
    # Get count of transformed records
    cursor.execute("SELECT COUNT(*) FROM staging.stg_weather_events WHERE data_quality_flag = 'VALID'")
    valid_count = cursor.fetchone()[0]
    
    # Also check invalid records
    cursor.execute("""
        SELECT data_quality_flag, COUNT(*) 
        FROM staging.stg_weather_events 
        GROUP BY data_quality_flag
    """)
    quality_counts = cursor.fetchall()
    logger.info(f"Data quality breakdown: {quality_counts}")
    
    logger.info(f"Transformed {valid_count} valid records to staging.stg_weather_events")
    return valid_count

def load_marts(conn):
    """Load data from staging to marts schema (facts and dimensions)"""
    logger.info("Loading data from STAGING to MARTS schema...")
    cursor = conn.cursor()
    
    # First, populate date dimension
    logger.info("Populating date dimension...")
    cursor.execute("""
    INSERT INTO marts.dim_date (date_key, full_date, year, quarter, month, month_name, 
                                day, day_of_week, day_name, week_of_year, is_weekend, season)
    SELECT DISTINCT
        (year * 10000 + month * 100 + day) as date_key,
        DATE(start_time_utc) as full_date,
        year,
        EXTRACT(QUARTER FROM start_time_utc) as quarter,
        month,
        TO_CHAR(start_time_utc, 'Month') as month_name,
        day,
        EXTRACT(DOW FROM start_time_utc) as day_of_week,
        TO_CHAR(start_time_utc, 'Day') as day_name,
        EXTRACT(WEEK FROM start_time_utc) as week_of_year,
        CASE WHEN EXTRACT(DOW FROM start_time_utc) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
        CASE 
            WHEN month IN (12, 1, 2) THEN 'Winter'
            WHEN month IN (3, 4, 5) THEN 'Spring'
            WHEN month IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END as season
    FROM staging.stg_weather_events
    WHERE start_time_utc IS NOT NULL
    ON CONFLICT (date_key) DO NOTHING;
    """)
    conn.commit()
    
    # Populate station dimension
    logger.info("Populating station dimension...")
    cursor.execute("""
    INSERT INTO marts.dim_station 
    (airport_code, timezone, latitude, longitude, city, county, state, region, valid_from)
    SELECT 
        airport_code,
        timezone,
        latitude,
        longitude,
        city,
        county,
        state,
        CASE 
            WHEN state IN ('WA', 'OR', 'CA') THEN 'West'
            WHEN state IN ('MT', 'ID', 'WY', 'NV', 'UT', 'CO', 'AZ', 'NM') THEN 'Southwest'
            WHEN state IN ('ND', 'SD', 'NE', 'KS', 'OK', 'TX') THEN 'Central'
            WHEN state IN ('MN', 'IA', 'MO', 'WI', 'IL', 'MI', 'IN', 'OH') THEN 'Midwest'
            WHEN state IN ('AR', 'LA', 'MS', 'AL', 'GA', 'FL', 'SC', 'NC', 'TN', 'KY', 'WV', 'VA') THEN 'Southeast'
            WHEN state IN ('PA', 'NY', 'NJ', 'CT', 'RI', 'MA', 'VT', 'NH', 'ME') THEN 'Northeast'
            ELSE 'Other'
        END as region,
        MIN(first_seen_date) as valid_from
    FROM staging.stg_stations
    GROUP BY airport_code, timezone, latitude, longitude, city, county, state
    ON CONFLICT (airport_code) DO UPDATE SET
        region = EXCLUDED.region,
        updated_at = CURRENT_TIMESTAMP;
    """)
    conn.commit()
    
    # Populate fact table
    logger.info("Populating fact table...")
    cursor.execute("""
    INSERT INTO marts.fact_weather_events 
    (event_id, station_id, date_key, type, severity, start_time_utc, end_time_utc,
     precipitation_in, duration_minutes, is_significant_event, severity_score, precipitation_category)
    SELECT 
        s.event_id,
        ds.station_id,
        (s.year * 10000 + s.month * 100 + s.day) as date_key,
        s.type,
        s.severity,
        s.start_time_utc,
        s.end_time_utc,
        s.precipitation_in,
        s.duration_minutes,
        CASE WHEN s.duration_minutes > 360 OR s.precipitation_in > 1 THEN TRUE ELSE FALSE END as is_significant_event,
        CASE s.severity
            WHEN 'Light' THEN 1
            WHEN 'Moderate' THEN 2
            WHEN 'Severe' THEN 3
            WHEN 'Heavy' THEN 4
            ELSE 0
        END as severity_score,
        CASE 
            WHEN s.precipitation_in = 0 THEN 'None'
            WHEN s.precipitation_in < 0.1 THEN 'Light'
            WHEN s.precipitation_in < 0.5 THEN 'Moderate'
            ELSE 'Heavy'
        END as precipitation_category
    FROM staging.stg_weather_events s
    JOIN marts.dim_station ds ON s.airport_code = ds.airport_code
    WHERE s.data_quality_flag = 'VALID'
    ON CONFLICT (event_id) DO UPDATE SET
        severity = EXCLUDED.severity,
        precipitation_in = EXCLUDED.precipitation_in,
        duration_minutes = EXCLUDED.duration_minutes;
    """)
    conn.commit()
    
    # Get counts
    cursor.execute("SELECT COUNT(*) FROM marts.fact_weather_events")
    fact_count = cursor.fetchone()[0]
    
    logger.info(f"Loaded {fact_count} records into marts.fact_weather_events")
    return fact_count

def main():
    """Main ETL function with schema layering"""
    try:
        conn = create_db_connection()
        
        # Step 1: Load raw data
        batch_id = 'ece5527d'
        
        # Step 2: Transform to staging
        valid_count = transform_raw_to_staging(conn, batch_id)
        
        # Step 3: Load to marts
        fact_count = load_marts(conn)
        
        conn.close()
        
        logger.info("=" * 50)
        logger.info("ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info(f"Raw layer: Data loaded (batch: {batch_id})")
        logger.info(f"Staging layer: {valid_count} valid records transformed")
        logger.info(f"Marts layer: {fact_count} records in fact table")
        logger.info("=" * 50)
        
        # Print sample query results
        print("\n=== Sample Data from Marts Layer ===")
        sample_query(conn)
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

def sample_query(conn):
    """Run a sample query to verify data"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT 
            type,
            severity,
            COUNT(*) as count,
            AVG(precipitation_in) as avg_precip,
            AVG(duration_minutes) as avg_duration
        FROM marts.fact_weather_events
        GROUP BY type, severity
        ORDER BY count DESC
        LIMIT 10;
        """)
        
        print("\nTop Weather Events by Type and Severity:")
        for row in cursor.fetchall():
            print(f"  {row[0]} - {row[1]}: {row[2]:,} events, Avg Precip: {row[3]:.2f}in, Avg Duration: {row[4]:.0f}min")
            
    except Exception as e:
        logger.warning(f"Could not run sample query: {e}")

if __name__ == "__main__":
    main()