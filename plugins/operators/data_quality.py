from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 table,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')

        self.log.info('Getting the Credentials')
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.table:
            self.log.info('Beginning the Data Quality operations on the table :{table}')
            records = redhsift_hook.get_records(f"SELECT Count(*) from {table};")

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality validation failed for the table : {table}.")
                raise ValueError(f"Data quality validation failed for the table : {table}")
            self.log.info(f"Data quality validation passed on table : {table}") 
