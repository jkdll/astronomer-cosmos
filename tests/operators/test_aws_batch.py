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
        job_queue = "my-job-queue",
        job_definition = "my-job-definition",
        operator_args={
                "aws_conn_id": "example_aws_conn_id",
                "job_name":"jaffle-shop",
                "job_queue":"my-job-queue",
                "job_definition":"my-job-definition",
        },
    )
    
    assert dbt_base_operator.add_global_flags == [
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]

@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_aws_batch_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtAwsBatchOperator(
        operator_args={
                "aws_conn_id": "example_aws_conn_id",
                "job_name":"jaffle-shop",
                "job_queue":"my-job-queue",
                "job_definition":"my-job-definition",
        },
    )
    dbt_base_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        ("tuple", "key"): "some_value",
    }
    p_context_to_airflow_vars.return_value = {"START_DATE": "2023-02-15 12:30:00"}
    env = dbt_base_operator.get_env(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    expected_env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "START_DATE": "2023-02-15 12:30:00",
    }
    assert env == expected_env

@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_aws_batch_operator_check_environment_variables(
    p_context_to_airflow_vars: MagicMock,
) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtAwsBatchOperator(
        operator_args={
                "aws_conn_id": "example_aws_conn_id",
                "job_name":"jaffle-shop",
                "job_queue":"my-job-queue",
                "job_definition":"my-job-definition",
        },
    )
    dbt_base_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        "FOO": "foo",
        ("tuple", "key"): "some_value",
    }
    expected_env = {"start_date": "20220101", "end_date": "20220102", "some_path": Path(__file__), "FOO": "BAR"}
    dbt_base_operator.build_command(context=MagicMock())

    assert dbt_base_operator.environment_variables == expected_env

base_kwargs = {
    "task_id": "my-task",
    "job_queue": "my-job-queue",
    "job_definition": "my-job-definition",
    "job_name": "my-job-name",
    "aws_conn_id": "my-aws-conn-id",
    "region_name": "us-east-1",
    "verify": True,
    "environment_variables": {"FOO": "BAR", "OTHER_FOO": "OTHER_BAR"},
    "container_overrides": {},
    "vcpus": 8,
    "memory": 1024,
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
    for command_name, command_operator in result_map.items():
        command_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
        if command_name not in {"run-operation", "source"}:
            assert command_operator.command == [
                "dbt",
                command_name,
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]
        elif command_name == "source":
            assert command_operator.command == [
                "dbt",
                command_name,
                "freshness",
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]
        else:
            assert command_operator.command == [
                "dbt",
                command_name,
                "some-macro",
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]

def test_dbt_aws_batch_container_overrides_command_only():
    """
    Check that command is populated if container_overrides is not specified
    """
    test_kwargs = {
        "job_queue":"my-job-queue",
        "job_definition":"my-job-definition",
        "job_name":"my-job-name",
        "aws_conn_id": "my-aws-conn-id",
    }
    run_operator = DbtRunAwsBatchOperator(**test_kwargs)
    run_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
    actual_container_overrides = run_operator.container_overrides

    assert "environment" not in actual_container_overrides
    assert "vcpus" not in actual_container_overrides
    assert "memory" not in actual_container_overrides
    assert "command" in actual_container_overrides

def test_dbt_aws_batch_environment_environment_only():
    """
    Check that environment variables passed to container_overrides if not specified
    """
    test_kwargs = {
        "job_queue":"my-job-queue",
        "job_definition":"my-job-definition",
        "job_name":"my-job-name",
        "aws_conn_id": "my-aws-conn-id",
        "environment_variables": [{"ENV_FOO":"ENV_BAR"}]
    }
    run_operator = DbtRunAwsBatchOperator(**test_kwargs)
    run_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
    actual_container_overrides = run_operator.container_overrides

    assert "command" in actual_container_overrides
    assert "environment" in actual_container_overrides
    assert "vcpus" not in actual_container_overrides
    assert "memory" not in actual_container_overrides
    assert len(actual_container_overrides["environment"]) == 1
    expected_env_vars = [
        {"name": "ENVIRONMENT_FOO", "value": "ENVIRONMENT_BAR"}
    ]
    assert all(env in actual_container_overrides["environment"] for env in expected_env_vars)

def test_dbt_aws_batch_container_overrides_all_values():
    """
    Check that all valid container overrides are passed successfully
    Check that merging of environment and container overrides is done correctly
    """
    test_kwargs = {
        "job_queue":"my-job-queue",
        "job_definition":"my-job-definition",
        "job_name":"my-job-name",
        "aws_conn_id": "my-aws-conn-id",
        "container_overrides": {"vcpus":999,"memory":999,"environment":[{"name":"CONTAINER_FOO","value":"CONTAINER_BAR"}]},
        "environment_variables": [{"ENV_FOO":"ENV_BAR"}]
    }
    run_operator = DbtRunAwsBatchOperator(**test_kwargs)
    run_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
    actual_container_overrides = run_operator.container_overrides
    assert "vcpus" in actual_container_overrides
    assert "memory" in actual_container_overrides
    assert actual_container_overrides["vcpus"] == 999
    assert actual_container_overrides["memory"] == 999
    assert len(actual_container_overrides["environment"]) == 2
    
    expected_env_vars = [
        {"name": "CONTAINER_FOO", "value": "CONTAINER_BAR"},
        {"name": "ENVIRONMENT_FOO", "value": "ENVIRONMENT_BAR"}
    ]
    assert all(env in actual_container_overrides["environment"] for env in expected_env_vars)

@patch("airflow.providers.amazon.aws.operators.batch")
@patch("cosmos.operators.base.AbstractDbtBase")
def test_dbt_aws_batch_kwargs_propogation(mock_base_init, mock_batch_init):
    mock_base_init.return_value = None
    mock_batch_init.return_value = None
    test_kwargs = {
        "job_queue":"my-job-queue",
        "job_definition":"my-job-definition",
        "job_name":"my-job-name",
        "aws_conn_id": "my-aws-conn-id",
        "container_overrides": {"vcpus":999,"memory":999,"environment":[{"name":"CONTAINER_FOO","value":"CONTAINER_BAR"}]},
        "environment_variables": [{"ENV_FOO":"ENV_BAR"}]
    }
    run_operator = DbtRunAwsBatchOperator(**test_kwargs)
    mock_batch_init.assert_called()
    batch_call_args = mock_batch_init.call_args[1]
    assert "job_queue" in batch_call_args
    assert "task_id" in batch_call_args

    base_call_args = mock_base_init.call_args[1]
    assert "job_definition" in base_call_args
    assert "job_name" in base_call_args
    

@patch("cosmos.operators.aws_batch.AwsBatchOperator.execute")
def test_dbt_aws_batch_build_and_run_cmd(mock_execute):
    """
    Check that building methods run correctly.
    """
    dbt_base_operator = ConcreteDbtAwsBatchOperator(
        operator_args={
                "aws_conn_id": "example_aws_conn_id",
                "job_name":"jaffle-shop",
                "job_queue":"my-job-queue",
                "job_definition":"my-job-definition",
        },
    )
    mock_build_command = MagicMock()
    dbt_base_operator.build_command = mock_build_command

    mock_context = MagicMock()
    dbt_base_operator.build_and_run_cmd(context=mock_context)

    mock_build_command.assert_called_with(mock_context, None)
    mock_execute.assert_called_once_with(dbt_base_operator, mock_context)