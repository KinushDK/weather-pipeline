"""
Weather Events ETL Pipeline DAG
---------------------------------
Orchestrates the monthly ETL pipeline:
  Task 1 → Transform raw → staging
  Task 2 → Load staging  → marts
  Task 3 → Verify counts
"""

from datetime import datetime, timedelta
import os
import logging
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


logger = logging.getLogger(__name__)

# ── DB Config ──────────────────────────────────────────────────────────────────
DB_CONFIG = {
    'host':     os.getenv('WEATHER_POSTGRES_HOST',     'host.docker.internal'),
    'database': os.getenv('WEATHER_POSTGRES_DB',       'weather_db'),
    'user':     os.getenv('WEATHER_POSTGRES_USER',     'postgres'),
    'password': os.getenv('WEATHER_POSTGRES_PASSWORD', 'sekonda'),
    'port':     int(os.getenv('WEATHER_POSTGRES_PORT', 5432)),
}

default_args = {
    'owner':            'data_team',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
}


# =============================================================================
# TASK 1 — Transform raw → staging
# =============================================================================
def transform_to_staging(**context):
    """Transform raw.weather_events_raw into staging.stg_weather_events"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        # Get latest batch_id from raw layer
        cursor.execute("""
            SELECT batch_id
            FROM raw.weather_events_raw
            GROUP BY batch_id
            ORDER BY MAX(ingestion_timestamp) DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        if not result:
            raise ValueError("No data found in raw.weather_events_raw")

        batch_id = result[0]
        logger.info(f"Processing batch_id: {batch_id}")

        # Set lock timeout to avoid deadlocks
        cursor.execute("SET lock_timeout = '30s';")
        #cursor.execute("SET lock_timeout = '5s';")

        # Truncate staging before reload
        cursor.execute("TRUNCATE TABLE staging.stg_weather_events CASCADE;")
        cursor.execute("TRUNCATE TABLE staging.stg_stations CASCADE;")
        conn.commit()

        # Transform raw → staging
        cursor.execute("""
            INSERT INTO staging.stg_weather_events
            (event_id, type, severity, start_time_utc, end_time_utc,
             precipitation_in, timezone, airport_code, location_lat, location_lng,
             city, county, state, zip_code, duration_minutes,
             year, month, day, hour, data_quality_flag)
            SELECT
                event_id,
                type,
                CASE
                    WHEN severity IN ('Light','Moderate','Severe','Heavy') THEN severity
                    ELSE 'Unknown'
                END as severity,
                CASE
                    WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                        THEN start_time_utc::TIMESTAMP
                    ELSE NULL
                END as start_time_utc,
                CASE
                    WHEN end_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                        THEN end_time_utc::TIMESTAMP
                    ELSE NULL
                END as end_time_utc,
                CASE
                    WHEN precipitation_in ~ '^[0-9.]+$'
                        THEN precipitation_in::DECIMAL(10,4)
                    ELSE 0
                END as precipitation_in,
                timezone,
                COALESCE(NULLIF(airport_code,''), 'UNKNOWN') as airport_code,
                CASE WHEN location_lat ~ '^-?[0-9.]+$'
                    THEN location_lat::DECIMAL(10,7) ELSE NULL END,
                CASE WHEN location_lng ~ '^-?[0-9.]+$'
                    THEN location_lng::DECIMAL(10,7) ELSE NULL END,
                COALESCE(NULLIF(city,''),   'Unknown') as city,
                COALESCE(NULLIF(county,''), 'Unknown') as county,
                COALESCE(NULLIF(state,''),  'Unknown') as state,
                COALESCE(zip_code, '') as zip_code,
                CASE
                    WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                     AND end_time_utc   ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                    THEN EXTRACT(EPOCH FROM (
                        end_time_utc::TIMESTAMP - start_time_utc::TIMESTAMP
                    ))/60
                    ELSE NULL
                END as duration_minutes,
                CASE WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    THEN EXTRACT(YEAR  FROM start_time_utc::TIMESTAMP) ELSE NULL END,
                CASE WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    THEN EXTRACT(MONTH FROM start_time_utc::TIMESTAMP) ELSE NULL END,
                CASE WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    THEN EXTRACT(DAY   FROM start_time_utc::TIMESTAMP) ELSE NULL END,
                CASE WHEN start_time_utc ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    THEN EXTRACT(HOUR  FROM start_time_utc::TIMESTAMP) ELSE NULL END,
                CASE
                    WHEN start_time_utc IS NULL OR end_time_utc IS NULL THEN 'INVALID_DATE'
                    WHEN start_time_utc !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 'INVALID_DATE_FORMAT'
                    WHEN severity NOT IN ('Light','Moderate','Severe','Heavy') THEN 'INVALID_SEVERITY'
                    ELSE 'VALID'
                END as data_quality_flag
            FROM raw.weather_events_raw
            WHERE batch_id = %s
            ON CONFLICT (event_id) DO UPDATE SET
                type              = EXCLUDED.type,
                severity          = EXCLUDED.severity,
                data_quality_flag = EXCLUDED.data_quality_flag;
        """, (batch_id,))
        conn.commit()

        # Populate staging stations
        cursor.execute("""
            INSERT INTO staging.stg_stations
            (airport_code, timezone, latitude, longitude,
             city, county, state, zip_code, first_seen_date, last_seen_date)
            SELECT
                airport_code,
                MAX(timezone)      as timezone,
                MAX(location_lat)  as latitude,
                MAX(location_lng)  as longitude,
                MAX(city)          as city,
                MAX(county)        as county,
                MAX(state)         as state,
                MAX(zip_code)      as zip_code,
                MIN(start_time_utc::DATE) as first_seen_date,
                MAX(start_time_utc::DATE) as last_seen_date
            FROM staging.stg_weather_events
            WHERE airport_code != 'UNKNOWN'
              AND start_time_utc IS NOT NULL
            GROUP BY airport_code
            ON CONFLICT (airport_code) DO UPDATE SET
                last_seen_date     = EXCLUDED.last_seen_date,
                staging_loaded_at  = CURRENT_TIMESTAMP;
        """)
        conn.commit()

        # Log quality breakdown
        cursor.execute("""
            SELECT data_quality_flag, COUNT(*)
            FROM staging.stg_weather_events
            GROUP BY data_quality_flag
            ORDER BY COUNT(*) DESC
        """)
        for row in cursor.fetchall():
            logger.info(f"  {row[0]}: {row[1]:,} rows")

        cursor.execute("""
            SELECT COUNT(*) FROM staging.stg_weather_events
            WHERE data_quality_flag = 'VALID'
        """)
        valid_count = cursor.fetchone()[0]
        logger.info(f"Staging complete: {valid_count:,} valid records")
        return valid_count

    finally:
        conn.close()


# =============================================================================
# TASK 2 — Load staging → marts
# =============================================================================
def load_marts(**context):
    """Load staging data into dim_date, dim_station, fact_weather_events"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        # ── Date dimension ────────────────────────────────────────────────
        logger.info("Populating dim_date...")
        cursor.execute("""
            INSERT INTO marts.dim_date
            (date_key, full_date, year, quarter, month, month_name,
             day, day_of_week, day_name, week_of_year, is_weekend, season)
            SELECT DISTINCT
                (year * 10000 + month * 100 + day)::INTEGER as date_key,
                DATE(start_time_utc)                        as full_date,
                year::INTEGER,
                EXTRACT(QUARTER FROM start_time_utc)::INTEGER as quarter,
                month::INTEGER,
                TO_CHAR(start_time_utc, 'Month')            as month_name,
                day::INTEGER,
                EXTRACT(DOW FROM start_time_utc)::INTEGER   as day_of_week,
                TO_CHAR(start_time_utc, 'Day')              as day_name,
                EXTRACT(WEEK FROM start_time_utc)::INTEGER  as week_of_year,
                CASE WHEN EXTRACT(DOW FROM start_time_utc)
                     IN (0,6) THEN TRUE ELSE FALSE END       as is_weekend,
                CASE
                    WHEN month IN (12,1,2) THEN 'Winter'
                    WHEN month IN (3,4,5)  THEN 'Spring'
                    WHEN month IN (6,7,8)  THEN 'Summer'
                    ELSE 'Fall'
                END as season
            FROM staging.stg_weather_events
            WHERE start_time_utc IS NOT NULL
              AND year IS NOT NULL
              AND month IS NOT NULL
              AND day IS NOT NULL
            ON CONFLICT (date_key) DO NOTHING;
        """)
        conn.commit()

        # ── Station dimension ─────────────────────────────────────────────
        logger.info("Populating dim_station...")
        cursor.execute("""
            INSERT INTO marts.dim_station
            (airport_code, timezone, latitude, longitude,
             city, county, state, region, valid_from)
            SELECT
                airport_code, timezone, latitude, longitude,
                city, county, state,
                CASE
                    WHEN state IN ('WA','OR','CA')
                        THEN 'West'
                    WHEN state IN ('MT','ID','WY','NV','UT','CO','AZ','NM')
                        THEN 'Southwest'
                    WHEN state IN ('ND','SD','NE','KS','OK','TX')
                        THEN 'Central'
                    WHEN state IN ('MN','IA','MO','WI','IL','MI','IN','OH')
                        THEN 'Midwest'
                    WHEN state IN ('AR','LA','MS','AL','GA','FL',
                                   'SC','NC','TN','KY','WV','VA')
                        THEN 'Southeast'
                    WHEN state IN ('PA','NY','NJ','CT','RI','MA','VT','NH','ME')
                        THEN 'Northeast'
                    ELSE 'Other'
                END as region,
                MIN(first_seen_date) as valid_from
            FROM staging.stg_stations
            GROUP BY airport_code, timezone, latitude,
                     longitude, city, county, state
            ON CONFLICT (airport_code) DO UPDATE SET
                region     = EXCLUDED.region,
                updated_at = CURRENT_TIMESTAMP;
        """)
        conn.commit()

        # ── Fact table ────────────────────────────────────────────────────
        logger.info("Populating fact_weather_events...")
        cursor.execute("""
            INSERT INTO marts.fact_weather_events
            (event_id, station_id, date_key, type, severity,
             start_time_utc, end_time_utc, precipitation_in,
             duration_minutes, is_significant_event,
             severity_score, precipitation_category)
            SELECT
                s.event_id,
                ds.station_id,
                (s.year * 10000 + s.month * 100 + s.day)::INTEGER as date_key,
                s.type,
                s.severity,
                s.start_time_utc,
                s.end_time_utc,
                s.precipitation_in,
                s.duration_minutes,
                CASE WHEN s.duration_minutes > 360
                      OR s.precipitation_in  > 1
                     THEN TRUE ELSE FALSE END as is_significant_event,
                CASE s.severity
                    WHEN 'Light'    THEN 1
                    WHEN 'Moderate' THEN 2
                    WHEN 'Severe'   THEN 3
                    WHEN 'Heavy'    THEN 4
                    ELSE 0
                END as severity_score,
                CASE
                    WHEN s.precipitation_in = 0   THEN 'None'
                    WHEN s.precipitation_in < 0.1 THEN 'Light'
                    WHEN s.precipitation_in < 0.5 THEN 'Moderate'
                    ELSE 'Heavy'
                END as precipitation_category
            FROM staging.stg_weather_events s
            JOIN marts.dim_station ds ON s.airport_code = ds.airport_code
            WHERE s.data_quality_flag = 'VALID'
            ON CONFLICT (event_id) DO UPDATE SET
                severity         = EXCLUDED.severity,
                precipitation_in = EXCLUDED.precipitation_in,
                duration_minutes = EXCLUDED.duration_minutes;
        """)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM marts.fact_weather_events")
        count = cursor.fetchone()[0]
        logger.info(f"Marts complete: {count:,} records in fact table")
        return count

    finally:
        conn.close()


# =============================================================================
# TASK 3 — Verify row counts
# =============================================================================
def verify_counts(**context):
    """Verify row counts across all layers"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        tables = [
            ('raw',      'raw.weather_events_raw'),
            ('staging',  'staging.stg_weather_events'),
            ('stations', 'marts.dim_station'),
            ('dates',    'marts.dim_date'),
            ('facts',    'marts.fact_weather_events'),
        ]
        logger.info("=" * 50)
        logger.info("ROW COUNT VERIFICATION")
        logger.info("=" * 50)
        for label, table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"  {label:<12} → {count:>12,} rows")
        logger.info("=" * 50)

    finally:
        conn.close()


# =============================================================================
# DAG definition
# =============================================================================
with DAG(
    dag_id='weather_etl_pipeline',
    description='Monthly weather events ETL pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='0 2 1 * *',  # 2 AM on 1st of every month
    catchup=False,
    tags=['weather', 'etl', 'postgres'],
) as dag:

    task_staging = PythonOperator(
        task_id='transform_to_staging',
        python_callable=transform_to_staging,
        doc_md='Transforms raw.weather_events_raw → staging.stg_weather_events',
    )

    task_marts = PythonOperator(
        task_id='load_marts',
        python_callable=load_marts,
        doc_md='Loads staging → dim_date, dim_station, fact_weather_events',
    )

    task_verify = PythonOperator(
        task_id='verify_counts',
        python_callable=verify_counts,
        doc_md='Verifies row counts across all layers',
    )

    # ── Pipeline order ────────────────────────────────────────────────────────
    task_staging >> task_marts >> task_verify