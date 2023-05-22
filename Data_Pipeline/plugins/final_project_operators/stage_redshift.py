from airflow.hooks.postgres_hook import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS json '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name = "",
                 s3_bucket="",
                 s3_key = "",
                 json_option = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_option = json_option
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        s3_location = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(
            f"Copying data to {self.table_name} staging table from location: {s3_location}"
        )
        
        copy_query = StageToRedshiftOperator.COPY_SQL.format(
            self.table_name,
            s3_location,
            aws_connection.login,
            aws_connection.password,
            self.json_option
        )
        
        self.log.info(f"Running copy query: {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift_hook.run(copy_query)
        self.log.info(f"Table {self.table_name} staged successfully!!")