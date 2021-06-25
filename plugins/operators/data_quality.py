from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
        Runs data quality check

    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_query="",
                 expected_result="",
                 *args, **kwargs):
        

       super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result

def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for i, query in enumerate(self.test_queries):
            rows = redshift_hook.get_records(query)