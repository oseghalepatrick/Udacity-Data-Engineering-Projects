from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):
    ui_color = '#ffd79d'

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id = "",
                 sql_create_query="",
                 *args, **kwargs):
        
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_create_query=sql_create_query
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Executing creating tables in Redshift')
        redshift.run(self.sql_create_query)
        
        self.log.info("Tables Successfully Created !!!")