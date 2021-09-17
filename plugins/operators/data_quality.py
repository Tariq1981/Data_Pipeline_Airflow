from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_test_stmt=[],
                 sql_test_conditions=[], ## example: ["{} > 0","{} == 0"]
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_test_stmt = sql_test_stmt
        self.sql_test_conditions = sql_test_conditions

    def execute(self, context):
        failed_count = 0
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i in range(0,len(self.sql_test_stmt)):
            records = redshift_hook.get_records(self.sql_test_stmt[i])
            if len(records) < 1 or len(records[0]) < 1:
                failed_count += 1
            else:
                condition = eval(self.sql_test_conditions[i].format(records[0][0]))
                if not(condition):
                    failed_count += 1
                
        if failed_count > 0:
            self.log.error(f"There are (failed_counts) failed test cases")
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        
        self.log.info(f"All tests cases have passed !!!")
        
        