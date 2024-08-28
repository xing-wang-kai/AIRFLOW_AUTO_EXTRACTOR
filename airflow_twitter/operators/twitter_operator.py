import sys
sys.path.append("airflow_twitter")

from airflow.models import BaseOperator, DAG, TaskInstance
import json
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago

class TwitterOperator(BaseOperator):
    def __init__(self, start_time, end_time, query, **kwargs):
        self.start_time = start_time
        self.end_time = end_time
        self.query = query
        super().__init__(**kwargs)
    
    def execute(self, context):
        end_time= self.end_time
        start_time = self.start_time

        query = self.query

        with open("extraction_twitter.json", 'w') as twitter_files:
            for pg in TwitterHook(start_time, end_time, query).run():
                json.dump(pg, twitter_files,ensure_ascii=False)
                twitter_files.write("/n")

if __name__ == "__main__":
    end_time= datetime.now()
    start_time = (datetime.now() + timedelta(-1)).date()

    query = "data engineer"

    with DAG (
        dag_id='Twitter_extractor',
        start_date=datetime.now()
    )  as dag:
        twitter_operator = TwitterOperator(
                                            query=query, 
                                            start_time=start_time, 
                                            end_time=end_time, 
                                            task_id='Operator_function'
                                            )
        twitter_instance = TaskInstance(task=twitter_operator)

        twitter_operator.execute(twitter_instance.task_id)