from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.create_table import CreateTablesOperator
from udacity.common import final_project_sql_statements
from udacity.common import create_tables


bucket = Variable.get('s3_bucket')
prefix1 = Variable.get('s3_prefix1')
prefix2 = Variable.get('s3_prefix2')

default_args = {
    'owner': 'patrick oseghale',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['oseghalepatrick53@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')

    create_redshift_tables = CreateTablesOperator(
        task_id="Create_tables",
        redshift_conn_id="redshift",
        sql_create_query=create_tables.CreateTables.tables
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credential_id='aws_credentials',
        redshift_conn_id='redshift',
        table_name='staging_events',
        s3_bucket=bucket,
        s3_key=prefix1,
        json_option=f's3://{bucket}/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_credential_id='aws_credentials',
        redshift_conn_id='redshift',
        table_name='staging_songs',
        s3_bucket=bucket,
        s3_key=prefix2,
        json_option=f's3://{bucket}/song_json_path.json'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql_query=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        sql_query=final_project_sql_statements.SqlQueries.user_table_insert,
        table_name='users',
        delete_load=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        sql_query=final_project_sql_statements.SqlQueries.song_table_insert,
        table_name='songs',
        delete_load=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        sql_query=final_project_sql_statements.SqlQueries.artist_table_insert,
        table_name='artists',
        delete_load=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        sql_query=final_project_sql_statements.SqlQueries.time_table_insert,
        table_name='time',
        delete_load=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays','artists','songs','time','users']
    )

    start_operator >> create_redshift_tables
    create_redshift_tables >> [
        stage_songs_to_redshift, 
        stage_events_to_redshift
    ] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks
    run_quality_checks >> end_operator
final_project_dag = final_project()