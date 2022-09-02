from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import conf
import os

class CreateTablesOperator(BaseOperator):

    ui_color = '#89DA59'
    select_query = 'SELECT COUNT(*) FROM {}'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_table_file="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.create_table_file=create_table_file

    def execute(self, context):
        self.log.info('DataQualityOperator start executing')
        redshift = PostgresHook(self.redshift_conn_id)        
        
        self.log.info(f'DataQualityOperator Retrieve the sql queries from {self.create_table_file}')
        f= open(os.path.join(conf.get('core','dags_folder'), self.create_table_file), 'r')
        create_tables_queries = f.read()
        f.close()
        self.log.info(f'DataQualityOperator creates table queries {create_tables_queries}')

        redshift.run(create_tables_queries)
        
        self.log.info(f'DataQualityOperator creates tables successfully')
        
        