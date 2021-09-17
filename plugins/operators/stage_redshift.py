from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_file_path",) # for templating the path to include month and year of execution
    copy_sql="""
    COPY {} FROM '{}' 
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS json '{}'
    COMPUPDATE OFF 
    STATUPDATE OFF;
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 s3_file_path="",
                 s3_con_id="", # Dummy AWS connection in order to user IAM role instead of the key and secret key
                 json_load_type="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_file_path = s3_file_path
        self.s3_con_id = s3_con_id
        self.json_load_type = json_load_type
        

    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.s3_con_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_file_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_load_type
        )
        redshift.run(formatted_sql)







