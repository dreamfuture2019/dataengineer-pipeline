from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    select_query = 'SELECT COUNT(*) FROM {}'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables

    def execute(self, context):
        self.log.info('DataQualityOperator start executing')
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift.get_records(select_query.format(table))
            if records is None or len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"DataQualityOperator check failed. No records present in destination table {table}")
                raise ValueError(f"DataQualityOperator check failed. No records present in destination table {table}")
            self.log.info(f"DataQualityOperator on table {table} check passed with {records[0][0]} records")