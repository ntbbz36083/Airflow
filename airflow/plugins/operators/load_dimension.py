from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
from helpers import create_tables

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 table = "",
                 ApporTrun = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.ApporTrun = ApporTrun
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(create_tables.users_table_create)
        redshift.run(create_tables.songs_table_create)
        redshift.run(create_tables.artists_table_create)
        redshift.run(create_tables.time_table_create)
        if self.ApporTrun == "Trun":
            self.log.info(f"Truncate {self.table} from destination Redshift table")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        if self.table == 'users':
            redshift.run(SqlQueries.user_table_insert)
        elif self.table == 'songs':
            redshift.run(SqlQueries.song_table_insert)
        elif self.table == 'time':
            redshift.run(SqlQueries.time_table_insert)
        else:
            redshift.run(SqlQueries.artist_table_insert)
        self.log.info('{} table loaded!'.format(self.table))
