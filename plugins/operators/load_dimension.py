from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_delete = "DELETE FROM {};"
    sql_insert = "INSERT INTO {} {};"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_mode="",
                 sql_select="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_mode = load_mode
        self.sql_select = sql_select

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.load_mode == "truncate" or self.load_mode == "overwrite":
            self.log.info(f"Clearing data from destination Redshift table {self.table}")
            formatted_sql = LoadDimensionOperator.sql_delete.format(self.table)
            redshift.run(formatted_sql)
        
        if self.load_mode == "overwrite" or self.load_mode == "append":
            formatted_sql = LoadDimensionOperator.sql_insert.format(self.table,self.sql_select)
            redshift.run(formatted_sql)
            logging.info(f"Table {self.table} has been loaded !!!")
        else:
            logging.error(f"Invalid load_mode {self.load_mode} !!!")
            raise ValueError(f"Invalid load_mode {self.load_mode} !!!")
            
        
        
        
