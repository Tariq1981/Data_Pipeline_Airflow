from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_insert = "INSERT INTO {} {};"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_select="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_select = sql_select

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        formatted_sql = LoadFactOperator.sql_insert.format(self.table,self.sql_select)
        redshift.run(formatted_sql)
        logging.info(f"Fact table {self.table} has been loaded !!!")
        