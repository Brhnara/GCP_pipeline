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
    'opensky_daily_analysis',
    default_args=default_args,
    description='Daily flight anlaysis',
    schedule_interval='0 0 * * *', 
    start_date=days_ago(1),
    catchup=False,
    tags=['opensky', 'flight', 'daily'],
)


daily_summary_query = """
WITH today_data AS (
    SELECT 
        DATE(TIMESTAMP_SECONDS(timestamp)) as day,
        COUNT(DISTINCT FORMAT("%d", timestamp)) as data_points,
        AVG(aircraft_count) as avg_aircraft_count,
        MAX(aircraft_count) as max_aircraft_count,
        MIN(aircraft_count) as min_aircraft_count
    FROM `{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.{{ var.value.bigquery_table }}`
    WHERE DATE(TIMESTAMP_SECONDS(timestamp)) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY day
),
yesterday_data AS (
    SELECT 
        DATE(TIMESTAMP_SECONDS(timestamp)) as day,
        AVG(aircraft_count) as avg_aircraft_count
    FROM `{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.{{ var.value.bigquery_table }}`
    WHERE DATE(TIMESTAMP_SECONDS(timestamp)) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    GROUP BY day
)
SELECT 
    t.day,
    t.data_points,
    t.avg_aircraft_count,
    t.max_aircraft_count,
    t.min_aircraft_count,
    y.avg_aircraft_count as yesterday_avg_aircraft_count,
    ROUND((t.avg_aircraft_count - y.avg_aircraft_count) / NULLIF(y.avg_aircraft_count, 0) * 100, 2) as daily_change_percent
FROM today_data t
LEFT JOIN yesterday_data y ON DATE_SUB(t.day, INTERVAL 1 DAY) = y.day
"""

create_daily_summary = BigQueryExecuteQueryOperator(
    task_id='create_daily_summary',
    sql=daily_summary_query,
    use_legacy_sql=False,
    destination_dataset_table='{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.daily_summary',
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    dag=dag,
)

busiest_hours_query = """
SELECT 
    DATE(TIMESTAMP_SECONDS(timestamp)) as day,
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(timestamp)) as hour,
    AVG(aircraft_count) as avg_aircraft_count,
    MAX(aircraft_count) as max_aircraft_count
FROM `{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.{{ var.value.bigquery_table }}`
WHERE DATE(TIMESTAMP_SECONDS(timestamp)) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY day, hour
ORDER BY avg_aircraft_count DESC
"""

create_busiest_hours = BigQueryExecuteQueryOperator(
    task_id='create_busiest_hours',
    sql=busiest_hours_query,
    use_legacy_sql=False,
    destination_dataset_table='{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.daily_busiest_hours',
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    dag=dag,
)


country_summary_query = """
WITH aircraft_states AS (
    SELECT 
        DATE(TIMESTAMP_SECONDS(timestamp)) as day,
        aircraft
    FROM `{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.{{ var.value.bigquery_table }}`
    WHERE DATE(TIMESTAMP_SECONDS(timestamp)) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),
unnested_aircraft AS (
    SELECT
        day,
        aircraft_info.origin_country as country,
        aircraft_info.icao24
    FROM aircraft_states,
    UNNEST(aircraft) as aircraft_info
    WHERE aircraft_info.origin_country IS NOT NULL
)
SELECT 
    day,
    country,
    COUNT(DISTINCT icao24) as unique_aircraft,
    COUNT(*) as total_observations
FROM unnested_aircraft
GROUP BY day, country
ORDER BY day, unique_aircraft DESC
LIMIT 100  -- En aktif 100 ülke
"""

create_country_summary = BigQueryExecuteQueryOperator(
    task_id='create_country_summary',
    sql=country_summary_query,
    use_legacy_sql=False,
    destination_dataset_table='{{ var.value.project_id }}.{{ var.value.bigquery_dataset }}.daily_country_summary',
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    dag=dag,
)



create_daily_summary >> create_busiest_hours >> create_country_summary 