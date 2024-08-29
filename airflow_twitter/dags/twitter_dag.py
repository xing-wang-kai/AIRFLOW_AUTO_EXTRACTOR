import sys
sys.path.append('airflow_twitter')

from airflow.models import DAG, TaskInstance
from os.path import join
from operators.twitter_operator import TwitterOperator
from airflow.utils.dates import days_ago

with DAG (
        
        dag_id='Twitter_extractor_DAGs',
        start_date= days_ago(7),
        schedule_interval= '@daily'

    )  as dag:
        
        query = "data engineer"

        twitter_operator = TwitterOperator(
                                            file_path=join("datalake/twitter_dataengineer", 
                                                            "extract_date={{ ds }}",
                                                            "data_engineer_{{ ds_nodash }}"),
                                            query=query,  
                                            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",

                                            task_id='Operator_function'
                                            )
 #       twitter_instance = TaskInstance(task=twitter_operator)
#
  #      twitter_operator.execute(twitter_instance.task_id)