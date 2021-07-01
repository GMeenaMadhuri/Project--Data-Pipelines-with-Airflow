from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table='',
                 redshift_conn_id='',
                 sql_query='',
                 truncate= False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')

        self.log.info("Getting the Credentials")
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            redhshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info('Loading data into dimension tables')
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
