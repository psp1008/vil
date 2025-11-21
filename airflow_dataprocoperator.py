# Python imports
import pendulum
import random
import string
import re

from datetime import timedelta
from airflow.operators.python import PythonOperator

# Airflow imports
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# e2eai library imports
from e2eai import alerting

'''
Pipeline Description

generate arrival by data for last mile delivery

'''


def task_failure_cb(context):
    is_critical = alerting.is_production_alert()
    alerting.send_notification(dag_run_id=context['run_id'],
                               should_page=is_critical,
                               pipeline_name='last_mile_delivery_arrival_by_daily',
                               is_critical=is_critical,
                               log_url=alerting.get_log_url(context),
                               details=alerting.get_details(context),
                               group='Invalid',
                               slack_channel=f"#{'dataeng-pipeline-errors' if is_critical else 'dataeng-pipeline-errors'}")


def run_id_format(run_id):
    return (re.sub('[^a-z0-9_-]', '', run_id))


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': task_failure_cb
}

with DAG(
        "last_mile_delivery_arrival_by_daily",
        start_date=pendulum.datetime(2024, 3, 28, tz="US/Central"),
        schedule_interval=None,
        max_active_runs=1,
        default_args=default_args,
        catchup=False,
        user_defined_macros={"run_id_format": run_id_format},
        template_searchpath='/home/airflow/gcs/dags/sql/') as dag:

	 task1 = DataprocCreateBatchOperator(
             task_id="always_on",
             project_id="wmt-last-mile-ds-dev",
             region="us-west1",
             batch_id="faas-"+"".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
             batch={
    "pyspark_batch" : {
        "args" : [
            "--env={{ var.value.faas_environment }}",
            "--date={{next_ds}}"
        ],
        "jar_file_uris" : [
            "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.28.0.jar"
        ],
        "main_python_file_uri" : "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/lmd_daily_data_metrics.py",
        "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"]
    },
    "labels" : {
        "dag_id" : "{{ run_id_format(dag.dag_id) }}",
        "dag_run_id" : "{{ run_id_format(run_id) }}",
        "task_id" : "{{ run_id_format(task.task_id) }}"
    },
    "environment_config" : {
        "execution_config" : {
            "service_account" : "svc-ds@wmt-last-mile-ds-dev.iam.gserviceaccount.com",
            "subnetwork_uri" : "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
        }
    }
} )

	 task2 = BigQueryInsertJobOperator(
             task_id="spark_zones",
             project_id="wmt-last-mile-ds-dev",
             configuration={
    "jobType" : "QUERY",
    "query" : {
        "query" : "{% include 'spark_zones.sql' %}",
        "useLegacySql" : False
    },
    "labels" : {
        "dag_id" : "{{ run_id_format(dag.dag_id) }}",
        "dag_run_id" : "{{ run_id_format(run_id) }}",
        "task_id" : "{{ run_id_format(task.task_id) }}"
    }
},
             impersonation_chain="svc-ds@wmt-last-mile-ds-dev.iam.gserviceaccount.com",
             location="US" )


	 task3 = DataprocCreateBatchOperator(
        task_id="get_trip_level_data",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_trip_level_data_for_all.py",
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "args": [
                    "--env=dev",
                    "--job=job1",
                    "--date={{next_ds}}",
                    "--zone_count=500"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
                   "runtime_config": {
                "properties": {
                    "spark.dataproc.scaling.version": "2",
                    "spark.driver.cores": "4",
                    "spark.dynamicAllocation.executorAllocationRatio": "0.5", 
                    "spark.dataproc.executor.compute.tier": "standard",
                    
                    "spark.executor.cores": "4",
                    "spark.executor.instances": "8",
                    "spark.driver.memory": "8g",  
                    "spark.executor.memory": "8g",  
                    
                    "spark.sql.adaptive.enabled": "true",  
                    "spark.sql.shuffle.partitions": "200", 
                    "spark.sql.autoBroadcastJoinThreshold": "50MB", 
                    "spark.default.parallelism": "200",
                    "spark.sql.files.maxPartitionBytes": "128MB", 
                    "spark.dataproc.dynamic.scale-up.enabled": "true",  
                    "spark.executor.memoryOverhead": "1g" 
                }           
            }, 
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })

	 task4 = DataprocCreateBatchOperator(
        task_id="get_supply_demand1",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_supply_demand.py",
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "args": [
                    "--env=dev",
                    "--job=job1",
                    "--date={{next_ds}}",
                    "--zone_count=50"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.dataproc.scaling.version" : "2",
                    "spark.driver.cores" : "4",
                    "spark.dynamicAllocation.executorAllocationRatio" : "1.0",

                    "spark.dataproc.executor.compute.tier" : "standard",
                    "spark.executor.cores" : "4",
                    "spark.executor.instances" : "2",
                    "spark.driver.memory" : "20g",
                    "spark.executor.memory" : "20g"
                }               
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })

	 task5 = DataprocCreateBatchOperator(
        task_id="get_supply_demand2",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_supply_demand.py",
                "args": [
                    "--env=dev",
                    "--job=job2",
                    "--date={{next_ds}}",
                    "--zone_count=50"
                ],
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.dataproc.scaling.version" : "2",
                    "spark.driver.cores" : "16",
                    "spark.dynamicAllocation.executorAllocationRatio" : "1.0",

                    "spark.dataproc.executor.compute.tier" : "premium",
                    "spark.executor.cores" : "8",
                    "spark.executor.instances" : "2",
                    "spark.driver.memory" : "80g",
                    "spark.executor.memory" : "80g"
                }               
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })
      

	 task6 = DataprocCreateBatchOperator(
        task_id="get_supply_demand3",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_supply_demand.py",
                "args": [
                    "--env=dev",
                    "--job=job3",
                    "--date={{next_ds}}",
                    "--zone_count=50"
                ],
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.dataproc.scaling.version" : "2",
                    "spark.driver.cores" : "16",
                    "spark.dynamicAllocation.executorAllocationRatio" : "1.0",
                    
                    "spark.dataproc.executor.compute.tier" : "premium",
                    "spark.executor.cores" : "8",
                    "spark.executor.instances" : "2",
                    "spark.driver.memory" : "80g",
                    "spark.executor.memory" : "80g"
                }               
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })




	 task7 = DataprocCreateBatchOperator(
        task_id="get_offer_attributes1",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_offer_attributes.py",
                "args": [
                    "--env=dev",
                    "--job=job1",
                    "--date={{next_ds}}",
                    "--zone_count=50"
                ],
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.dataproc.scaling.version" : "2",
                    "spark.driver.cores" : "16",
                    "spark.dynamicAllocation.executorAllocationRatio" : "1.0",
                    "spark.executor.cores" : "8",
                    "spark.dataproc.executor.compute.tier" : "premium",
                    "spark.executor.instances" : "2",
                    "spark.driver.memory" : "40g",
                    "spark.executor.memory" : "40g"
                }
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })



	 task8 = DataprocCreateBatchOperator(
        task_id="get_offer_attributes2",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_offer_attributes.py",
                "args": [
                    "--env=dev",
                    "--job=job2",
                    "--date={{next_ds}}",
                    "--zone_count=50"
                ],
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.dataproc.scaling.version" : "2",
                    "spark.driver.cores" : "16",
                    "spark.dynamicAllocation.executorAllocationRatio" : "1.0",
                    "spark.executor.cores" : "8",
                    "spark.dataproc.executor.compute.tier" : "premium",
                    "spark.executor.instances" : "2",
                    "spark.driver.memory" : "80g",
                    "spark.executor.memory" : "80g"
                }
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })

	 task9 = DataprocCreateBatchOperator(
        task_id="get_offer_attributes3",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_offer_attributes.py",
                "args": [
                    "--env=dev",
                    "--job=job3",
                    "--date={{next_ds}}",
                    "--zone_count=50"
                ],
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.dataproc.scaling.version" : "2",
                    "spark.driver.cores" : "16",
                    "spark.dynamicAllocation.executorAllocationRatio" : "1.0",
                    "spark.executor.cores" : "8",
                    "spark.dataproc.executor.compute.tier" : "premium",
                    "spark.executor.instances" : "2",
                    "spark.driver.memory" : "80g",
                    "spark.executor.memory" : "80g"
                }
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })

	 task10 = DataprocCreateBatchOperator(
        task_id="get_offer_attributes4",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_offer_attributes.py",
                "args": [
                    "--env=dev",
                    "--job=job4",
                    "--date={{next_ds}}",
                    "--zone_count=50"
                ],
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.dataproc.scaling.version" : "2",
                    "spark.driver.cores" : "16",
                    "spark.dynamicAllocation.executorAllocationRatio" : "1.0",
                    "spark.executor.cores" : "8",
                    "spark.dataproc.executor.compute.tier" : "premium",
                    "spark.executor.instances" : "2",
                    "spark.driver.memory" : "80g",
                    "spark.executor.memory" : "80g"
                }
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })
      
	 task11 = DataprocCreateBatchOperator(
        task_id="copy_supply_demand_offer_attributes_from_temp",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_supply_demand_offer_attributes_from_temp.py",
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "args": [
                    "--env=dev"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })      


	 task12 = DataprocCreateBatchOperator(
        task_id="get_model_data1",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_model_data.py",
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "args": [
                    "--env=dev",
                    "--job=job1",
                    "--date={{next_ds}}",
                    "--zone_count=100"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.executor.instances": "2",
                    "spark.driver.cores": "4",
                    "spark.executor.cores": "4",
                    "spark.dynamicAllocation.executorAllocationRatio": "1.0",
                    "spark.dataproc.scaling.version": "2"
                }
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })

	 task13 = DataprocCreateBatchOperator(
        task_id="get_model_data2",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_model_data.py",
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "args": [
                    "--env=dev",
                    "--job=job2",
                    "--date={{next_ds}}",
                    "--zone_count=100"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.executor.instances": "2",
                    "spark.driver.cores": "8",
                    "spark.executor.cores": "8",
                    "spark.dynamicAllocation.executorAllocationRatio": "1.0",
                    "spark.dataproc.scaling.version": "2"
                }
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })

	 task14 = DataprocCreateBatchOperator(
        task_id="get_model_data3",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-" + "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/get_model_data.py",
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
                "args": [
                    "--env=dev",
                    "--job=job3",
                    "--date={{next_ds}}",
                    "--zone_count=100"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ]
            },
            "runtime_config": {
                "properties": {
                    "spark.executor.instances": "2",
                    "spark.driver.cores": "16",
                    "spark.executor.cores": "8",
                    "spark.dynamicAllocation.executorAllocationRatio": "1.0",
                    "spark.dataproc.scaling.version": "2"
                }
            },
            "labels": {
                "dag_id": "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id": "{{ run_id_format(run_id) }}",
                "task_id": "{{ run_id_format(task.task_id) }}"
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri": "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        })
	 task15 = DataprocCreateBatchOperator(
        task_id="model_data_duplicates",
        project_id="wmt-last-mile-ds-{{ var.value.faas_environment }}",
        region="us-west1",
        batch_id="faas-"+"".join(random.choice(string.ascii_lowercase + string.digits) for i in range(35)),
        batch={
            "pyspark_batch": {
                "args": [
                    "--env=dev"
                ],
                "jar_file_uris" : [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.0.jar"
                ],
                "main_python_file_uri" : "gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/model_data_duplicates.py",
                "python_file_uris": ["gs://wmt-last-mile-ds-{{ var.value.faas_environment }}/arrival_by/utils.py"],
            },
            "labels" : {
                "dag_id" : "{{ run_id_format(dag.dag_id) }}",
                "dag_run_id" : "{{ run_id_format(run_id) }}",
                "task_id" : "{{ run_id_format(task.task_id) }}"
            },
            "environment_config" : {
                "execution_config" : {
                    "service_account" : "svc-ds@wmt-last-mile-ds-{{ var.value.faas_environment }}.iam.gserviceaccount.com",
                    "subnetwork_uri" : "https://www.googleapis.com/compute/alpha/projects/shared-vpc-admin/regions/us-west1/subnetworks/prod-us-west1-01"
                }
            }
        } )


task1 >> task2 >> [task3, task4, task5, task6, task7, task8, task9,task10]

for task in [task3, task4, task5, task6, task7, task8, task9,task10]:
    task >> task11

task11 >> [task12,task13,task14] >> task15
