from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_sql = "TRUNCATE TABLE {}"
    insert_sql = """
                    INSERT INTO {}
                    {}
                """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.append=append
        self.table=table

    def execute(self, context):
        self.log.info('LoadDimensionOperator start executing')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append:
            self.log.info(f"LoadDimensionOperator start truncating table {self.table}")
            redshift.run(truncate_sql.format(self.table))
            self.log.info(f"LoadDimensionOperator truncated table {self.table} successfully")
            
        formatted_sql = self.insert_sql.format(self.table, self.sql_query)
        redshift.run(formatted_sql)
        self.log.info(f"LoadDimensionOperator insert into table {self.table} successfully")
