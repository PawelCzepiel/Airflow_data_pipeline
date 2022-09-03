from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append_data="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_data = append_data

    def execute(self, context):
        self.log.info(f"Loading {self.table} fact table") 
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        sql_statement_app = "INSERT INTO {} {}".format(self.table, self.sql)  
        sql_statement_trun = "TRUNCATE {}".format(self.table)
        if self.append_data:
            self.log.info(f"Appending the data to {self.table} fact table")
            redshift_hook.run(sql_statement_app)
        else:
            self.log.info(f"Cleaning {self.table} fact table")   
            redshift_hook.run(sql_statement_trun)            
            self.log.info(f"Loading {self.table} fact table")               
            redshift_hook.run(sql_statement_app)          