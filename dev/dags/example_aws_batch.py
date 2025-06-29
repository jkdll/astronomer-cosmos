"""
Example of cleanup DAG that can be used to clear cache originated from running the dbt ls command while
parsing the DbtDag or DbtTaskGroup since Cosmos 1.5.
"""

# [START cache_example]
import os
import json
from pathlib import Path
from airflow import DAG
from pendulum import datetime
from airflow.hooks.base import BaseHook

from cosmos import (
    DbtDag,
    ExecutionMode,
    DbtSeedAwsBatchOperator,
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROFILES_DIR = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT = 'jaffle_shop'
EXECUTION_MODE = ExecutionMode.AWS_BATCH

DEFAULT_AWS_CONN_ID = "example_aws_conn_id"
conn = BaseHook.get_connection(DEFAULT_AWS_CONN_ID)
conn_extra = json.loads(conn.extra)
AWS_ACCOUNT_ID = conn_extra.get('aws_account_id', '00000000000')
AWS_REGION_NAME = conn_extra.get('region_name', 'us-east-1')
JOB_QUEUE_NAME = "batch-queue-data-demo"
JOB_DEFINITION_NAME = "example-job-definition"


profile_config = ProfileConfig(profile_name="default", target_name="dev", profile_mapping=PostgresUserPasswordProfileMapping(
    conn_id="example_conn" if EXECUTION_MODE in (
        ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV) else "remote_con",
    profile_args={"schema": "public"},
    disable_event_tracking=True
))

aws_batch_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_ROOT_PATH / DBT_PROJECT,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(execution_mode=EXECUTION_MODE),
    operator_args={
        "aws_conn_id": DEFAULT_AWS_CONN_ID,
        # "region_name" : AWS_REGION_NAME # region_name must be specified if not already in the aws connection id
        "job_name": "jaffle-shop",
        "job_queue": f"arn:aws:batch:{AWS_REGION_NAME}:{AWS_ACCOUNT_ID}:job-queue/{JOB_QUEUE_NAME}",
        "job_definition": f"arn:aws:batch:{AWS_REGION_NAME}:{AWS_ACCOUNT_ID}:job-definition/{JOB_DEFINITION_NAME}",
    },
    # normal dag parameters
    dag_id="example_aws_batch",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 0},
)
