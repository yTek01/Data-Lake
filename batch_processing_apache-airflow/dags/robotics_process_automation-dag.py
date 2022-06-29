import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',    
    'retry_delay': timedelta(minutes=5),
}

rpa_dag = DAG(
        dag_id = "rpa",
        default_args=default_args,
        schedule_interval='@daily',	
        dagrun_timeout=timedelta(minutes=60),
        description='Run RPA container using Docker Operator',
        start_date = airflow.utils.dates.days_ago(1)
)

start = DummyOperator(task_id="start", dag=rpa_dag)

rpa_operator = DockerOperator(
    task_id='RPA_docker_runner',
    container_name='robotics_automation',
    image='rpa_selenium_image:latest', 
    command='python3 rpa_script.py', 
    docker_url='unix://var/run/docker.sock', 
    network_mode='bridge', 
    xcom_all=True, 
    auto_remove=True, 
    mount_tmp_dir=False,
    dag=rpa_dag
)

end = DummyOperator(task_id="end", dag=rpa_dag)

start >> rpa_operator >> end