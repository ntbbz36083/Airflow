from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        table_name = ['artists','users','songs','time','songplays']
        for i in table_name:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(i))
            #self.log.info(records)
            column_names = redshift.get_records("select column_name from information_schema.columns where table_name='{}'".format(i))
            #self.log.info(column_names)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(i))    
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. {} contained 0 rows".format(i))
            for j in range(len(column_names)):
                column_name = column_names[j][0]
                null_value = redshift.get_records("SELECT COUNT(*) FROM {} where {} is null".format(i, column_name))
                if len(null_value) > 0 and len(null_value[0])>0:
                    self.log.info("Data quality check failed. table {} column {} has null value".format(i,column_name))
            self.log.info("Data quality on table {} check passed with {} records".format(i,records[0][0]))