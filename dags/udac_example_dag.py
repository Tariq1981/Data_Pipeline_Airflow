from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    s3_file_path="s3://udacity-dend/log_data",
    s3_iamrole="arn:aws:iam::993373426926:role/dwhRole",
    s3_dummy_con_id="s3_dummy", 
    json_load_type="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_file_path="s3://udacity-dend/song_data",
    s3_iamrole="arn:aws:iam::993373426926:role/dwhRole",
    s3_dummy_con_id="s3_dummy"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,    
    redshift_conn_id="redshift",
    table="songplays",
    sql_select=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    load_mode="overwrite",
    sql_select=SqlQueries.user_table_insert
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    load_mode="overwrite",
    sql_select=SqlQueries.song_table_insert
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    load_mode="overwrite",
    sql_select=SqlQueries.artist_table_insert
    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    load_mode="overwrite",
    sql_select=SqlQueries.time_table_insert
    
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    sql_test_stmt=["SELECT COUNT(*) FRO public.artists"],
    sql_test_conditions=["{} > 0"]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

