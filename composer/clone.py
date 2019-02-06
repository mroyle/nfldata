from datetime import datetime, timedelta

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

repo_url = "https://github.com/ryurko/nflscrapR-data.git"#Variable.get("git_remote_url")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 8, 18, 30),
    'retries': 0,
    'email': ['mike@doit-intl.com'],
    'email_on_failure': True,
    'catchup': False,
    'depends_on_past': False,
}

name = 'git_sync'

schedule = '0 0 * * 3'

dag = DAG(name, schedule_interval=schedule, default_args=default_args)

git_sync_bash = """
    git clone {repo_url}
    gsutil -m rsync -r -d nflscrapR-data gs://nfl-raw-data/scrapR
""".format(repo_url=repo_url)

data_flow_bash = """ 
	gsutil cp gs://nfl-raw-data/nfl_data_flow.jar .
	java -jar nfl_data_flow.jar --pipeline=PlayerStats --runner=DataflowRunner --project=mike-playground-225822 --zone=us-central1-a --tempLocation=gs://dataflow-staging-us-central1-1031337770488/temp/  --stagingLocation=gs://nfl-raw-data/dataflow-staging --input=gs://nfl-raw-data/scrapR/play_by_play_data/regular_season/reg_pbp_*.csv --output=mike-playground-225822:NFLData.player_results_by_game --bigTableInstanceID=nfl-bigtable --bigTableName=raw-data

"""

clone_git = BashOperator(
    task_id= 'git_pull',
    bash_command=git_sync_bash,
    dag=dag
)

transform_push_to_bigquery = BashOperator(
    task_id= 'transform_push',
    bash_command=data_flow_bash,
    dag=dag
)

transform_push_to_bigquery.set_upstream(clone_git)
