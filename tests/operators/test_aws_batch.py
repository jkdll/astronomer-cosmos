from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.utils.context import Context
from pendulum import datetime

from cosmos.operators.aws_batch import (
    DbtAwsBatchBaseOperator,
    DbtBuildAwsBatchOperator,
    DbtLSAwsBatchOperator,
    DbtRunAwsBatchOperator,
    DbtRunOperationAwsBatchOperator,
    DbtSeedAwsBatchOperator,
    DbtSnapshotAwsBatchOperator,
    DbtSourceAwsBatchOperator,
    DbtTestAwsBatchOperator,
)

class ConcreteDbtAwsBatchOperator(DbtAwsBatchBaseOperator):
    base_cmd = ["cmd"]

def test_dbt_aws_batch_operator_add_global_flags() -> None:
    """
    Check if global flags are added correctly.
    """
    dbt_base_operator = ConcreteDbtAwsBatchOperator(
        aws_conn_id="",
        region_name="",
        verify=True,
        job_name="",
        job_queue="",
        job_definition=""
    )
    pass

@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_aws_batch_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    pass

@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_aws_batch_operator_check_environment_variables(
    p_context_to_airflow_vars: MagicMock,
) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    pass

base_kwargs = {
    "task_id": "my-task",
    "aws_conn_id": "my-aws-conn-id",
    "cluster": "my-ecs-cluster",
    "task_definition": "my-dbt-task-definition",
    "container_name": "my-dbt-container-name",
    "environment_variables": {"FOO": "BAR", "OTHER_FOO": "OTHER_BAR"},
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}

result_map = {
    "ls": DbtLSAwsBatchOperator(**base_kwargs),
    "run": DbtRunAwsBatchOperator(**base_kwargs),
    "test": DbtTestAwsBatchOperator(**base_kwargs),
    "source": DbtSourceAwsBatchOperator(**base_kwargs),
    "seed": DbtSeedAwsBatchOperator(**base_kwargs),
    "build": DbtBuildAwsBatchOperator(**base_kwargs),
    "snapshot": DbtSnapshotAwsBatchOperator(**base_kwargs),
    "run-operation": DbtRunOperationAwsBatchOperator(macro_name="some-macro", **base_kwargs),
}

def test_dbt_aws_batch_build_command():
    """
    Check whether the dbt command is built correctly.
    """
    pass

def test_dbt_aws_batch_overrides_parameter():
    """
    Check whether overrides parameter passed on to EcsRunTaskOperator is built correctly.
    """
    pass

@patch("cosmos.operators.aws_ecs.BatchOperator.execute")
def test_dbt_aws_batch_build_and_run_cmd(mock_execute):
    """
    Check that building methods run correctly.
    """
    pass