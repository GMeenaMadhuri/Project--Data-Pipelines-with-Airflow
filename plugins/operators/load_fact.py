from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table='',
                 redshift_conn_id='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')

        self.log.info('Getting the Credentials')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Loading data into fact table')
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
