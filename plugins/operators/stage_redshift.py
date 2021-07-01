from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table ="",
                 redshift_conn_id ="",
                 aws_credentials_id ="",
                 s3_bucket ="",
                 s3_key ="",
                 json_option ="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_option = json_option

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')

        self.log.info('Extracting Credentails')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing the data from Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('Copying data from S3 to Redshift')
        self.s3_key = self.s3_key.format(**context)

        s3_path = (f"s3://{self.s3_bucket}/{self.s3_key}")
        redshift.run(f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' \
            SECRET_ACCESS_KEY '{credentials.secret_key}' FORMAT AS JSON '{self.json_option}'")
