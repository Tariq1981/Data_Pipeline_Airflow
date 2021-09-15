from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_test_stmt=[],
                 sql_test_result=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        failed_count = 0
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i in range(0,len(self.sql_test_stmt)):
            records = redshift_hook.get_records(self.sql_test_stmt[i])
            if len(records) < 1 or len(records[0]) != self.sql_test_result[i]:
                failed_count += 1
                
        if failed_count > 0:
            logging.error(f"There are (failed_counts) failed test cases")
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        
        logging.info(f"All tests cases have passed !!!")
        
        