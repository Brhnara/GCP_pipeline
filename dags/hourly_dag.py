
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'opensky_hourly_analysis',
    default_args=default_args,
    description='Hourly flight anlysis',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['opensky', 'flight', 'hourly'],
)


hourly_summary_query = """
SELECT 
    TIMESTAMP_TRUNC(TIMESTAMP_SECONDS(timestamp), HOUR) as hour,
    COUNT(DISTINCT FORMAT("%d", timestamp)) as data_points,
    AVG(aircraft_count) as avg_aircraft_count,
    MAX(aircraft_count) as max_aircraft_count,
    MIN(aircraft_count) as min_aircraft_count
FROM `{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.{{ var.value.bigquery_table }}`
WHERE timestamp >= CAST(UNIX_SECONDS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS INT64)
GROUP BY hour
ORDER BY hour DESC
"""

create_hourly_summary = BigQueryExecuteQueryOperator(
    task_id='create_hourly_summary',
    sql=hourly_summary_query,
    use_legacy_sql=False,
    destination_dataset_table='{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.hourly_summary',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    dag=dag,
)


country_distribution_query = """
WITH aircraft_states AS (
    SELECT 
        TIMESTAMP_SECONDS(timestamp) as timestamp_ts,
        aircraft
    FROM `{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.{{ var.value.bigquery_table }}`
    WHERE timestamp >= CAST(UNIX_SECONDS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS INT64)
),
unnested_aircraft AS (
    SELECT
        TIMESTAMP_TRUNC(timestamp_ts, HOUR) as hour,
        aircraft_info
    FROM aircraft_states,
    UNNEST(aircraft) as aircraft_info
)
SELECT 
    hour,
    aircraft_info.origin_country as country,
    COUNT(*) as aircraft_count,
    ROUND(AVG(aircraft_info.altitude), 2) as avg_altitude,
    ROUND(AVG(aircraft_info.velocity), 2) as avg_velocity
FROM unnested_aircraft
WHERE aircraft_info.origin_country IS NOT NULL
GROUP BY hour, country
ORDER BY hour DESC, aircraft_count DESC
"""

create_country_distribution = BigQueryExecuteQueryOperator(
    task_id='create_country_distribution',
    sql=country_distribution_query,
    use_legacy_sql=False,
    destination_dataset_table='{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.hourly_country_stats',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    dag=dag,
)


flight_metrics_query = """
WITH aircraft_states AS (
    SELECT 
        TIMESTAMP_SECONDS(timestamp) as timestamp_ts,
        aircraft
    FROM `{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.{{ var.value.bigquery_table }}`
    WHERE timestamp >= CAST(UNIX_SECONDS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS INT64)
),
unnested_aircraft AS (
    SELECT
        TIMESTAMP_TRUNC(timestamp_ts, HOUR) as hour,
        aircraft_info
    FROM aircraft_states,
    UNNEST(aircraft) as aircraft_info
)
SELECT 
    hour,
    COUNT(DISTINCT aircraft_info.icao24) as unique_aircraft,
    ROUND(AVG(CASE WHEN aircraft_info.altitude > 0 THEN aircraft_info.altitude ELSE NULL END), 2) as avg_altitude_meters,
    ROUND(MIN(CASE WHEN aircraft_info.altitude > 0 THEN aircraft_info.altitude ELSE NULL END), 2) as min_altitude_meters,
    ROUND(MAX(aircraft_info.altitude), 2) as max_altitude_meters,
    ROUND(AVG(CASE WHEN aircraft_info.velocity > 0 THEN aircraft_info.velocity ELSE NULL END), 2) as avg_velocity_ms,
    ROUND(MAX(aircraft_info.velocity), 2) as max_velocity_ms
FROM unnested_aircraft
GROUP BY hour
ORDER BY hour DESC
"""

create_flight_metrics = BigQueryExecuteQueryOperator(
    task_id='create_flight_metrics',
    sql=flight_metrics_query,
    use_legacy_sql=False,
    destination_dataset_table='{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.hourly_flight_metrics',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    dag=dag,
)


create_hourly_summary >> create_country_distribution >> create_flight_metrics