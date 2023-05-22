from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table_name="",
                 delete_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.delete_load = delete_load

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.delete_load:
            self.log.info(f"Deleting records on {self.table_name} dimension table !!")
            redshift_hook.run(f"DELETE FROM {self.table_name};")
            
        self.log.info(f"Loading data in {self.table_name} dimension table")
        redshift_hook.run(self.sql_query)
        self.log.info(f"Dimension Table {self.table_name} loaded successfully !!!")
