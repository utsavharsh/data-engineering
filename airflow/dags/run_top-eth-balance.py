#!/usr/bin/env python

from airflow import DAG

from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)

PROJECT_ID = "nodal-deck-367512"
CLUSTER_NAME = "crypto-eth-dataproc"
REGION = "us-west4"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},

    },
}

args = {
        'start_date': '2022-11-05',
        'dataproc_pyspark_jars': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar']
        }
dag = DAG(
        dag_id='top-eth-balance',
        default_args=args,
        description='To find top ethereum balances from traces of public data set in Big Query',
        schedule_interval='@daily'
        )

with dag:
    create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            dag=dag,
            project_id=PROJECT_ID,
            cluster_config=CLUSTER_CONFIG,
            region=REGION,
            cluster_name=CLUSTER_NAME
            )

    pyspark_job = DataProcPySparkOperator(
            task_id='pyspark_task',
            dag=dag,
            main='gs://spark-scripts/top-eth-balance.py',
            cluster_name=CLUSTER_NAME,
            region=REGION,
            job_name='top-eth-balance',
            dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar']
            )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        dag=dag,
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    ) 

    create_cluster >> pyspark_job >> delete_cluster
        