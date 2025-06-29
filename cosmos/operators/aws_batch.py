from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Callable, Sequence

if TYPE_CHECKING:  # pragma: no cover
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined]

from cosmos.config import ProfileConfig
from cosmos.log import get_logger
from cosmos.operators.base import (
    AbstractDbtBase,
    DbtBuildMixin,
    DbtLSMixin,
    DbtRunMixin,
    DbtRunOperationMixin,
    DbtSeedMixin,
    DbtSnapshotMixin,
    DbtSourceMixin,
    DbtTestMixin,
)

logger = get_logger(__name__)

DEFAULT_CONN_ID = "aws_default"
DEFAULT_JOB_NAME = "dbt"
DEFAULT_TAGS: dict[str, str] = {}


try:
    from airflow.sdk.bases.operator import BaseOperator  # Airflow 3
except ImportError:
    from airflow.models import BaseOperator  # Airflow 2

try:
    from airflow.sdk.bases.hooks.base import BaseHook  # Airflow 3
except ImportError:
    from airflow.hooks.base import BaseHook  # Airflow 2

try:
    from airflow.providers.amazon.aws.operators.batch import BatchOperator
except ImportError:  # pragma: no cover
    raise ImportError(
        "Could not import BatchOperator. Ensure you've installed the Amazon Web Services provider "
        "separately or with `pip install astronomer-cosmos[...,aws-ecs]`."
    )  # pragma: no cover


class DbtAwsBatchBaseOperator(AbstractDbtBase, BatchOperator):
    """
    Executes a dbt core cli command in an AWS Batch Task with dbt installed on it.

    """

    template_fields: Sequence[str] = tuple(
        list(AbstractDbtBase.template_fields) +
        list(BatchOperator.template_fields)
    )

    def __init__(
            self,
            job_queue: str,
            job_definition: str,
            job_name: str = DEFAULT_JOB_NAME,
            # AWS Connectivity
            aws_conn_id: str = DEFAULT_CONN_ID,
            region_name: str = None,
            verify: bool = None,            
            # Other Arguments
            environment_variables: dict[str, Any] | None = None,
            container_overrides: dict[str, Any] | None = None,
            # container_overrides can be overriden with these paramaeters
            vcpus: int = None,
            memory: int = None,
            #
            profile_config: ProfileConfig | None = None,
            command: list[str] | None = None,
            **kwargs: Any,
    ) -> None:
        self.profile_config = profile_config
        self.command = command
        self.job_name = job_name
        self.job_queue = job_queue
        self.job_definition = job_definition
        self.environment_variables = environment_variables or {}
        self.container_overrides = container_overrides
        self.vcpus = vcpus
        self.memory = memory
        # Set containerOverrides;
        # Either take input dictionary or distinct arguments.
        container_details = {}
        if self.command:
            container_details["command"] = self.command
        if self.vcpus:
            container_details["vcpus"] = self.vcpus
        if self.memory:
            container_details["memory"] = self.memory
        if self.environment_variables:
            container_details["environment"] = [
                {"name": key, "value": value} for key, value in self.environment_variables.items()]
        self.container_overrides = {
            **(self.container_overrides or {}),
            **container_details
        }
        kwargs.update(
            {
                "aws_conn_id": aws_conn_id,
                "region_name": region_name,
                "verify": verify,
                "job_name": job_name,
                "job_queue": job_queue,
                "job_definition": job_definition,
                "container_overrides": self.container_overrides
            }
        )

        # In PR #1474, we refactored cosmos.operators.base.AbstractDbtBase to remove its inheritance from BaseOperator
        # and eliminated the super().__init__() call. This change was made to resolve conflicts in parent class
        # initializations while adding support for ExecutionMode.AIRFLOW_ASYNC. Operators under this mode inherit
        # Airflow provider operators that enable deferrable SQL query execution. Since super().__init__() was removed
        # from AbstractDbtBase and different parent classes require distinct initialization arguments, we explicitly
        # initialize them (including the BaseOperator) here by segregating the required arguments for each parent class.
        default_args = kwargs.get("default_args", {})
        operator_kwargs = {}

        operator_args: set[str] = set()
        for clazz in BatchOperator.__mro__:
            operator_args.update(inspect.signature(
                clazz.__init__).parameters.keys())
            if clazz == BaseOperator:
                break
        for arg in operator_args:
            try:
                operator_kwargs[arg] = kwargs[arg]
            except KeyError:
                pass

        base_kwargs = {}
        for arg in {*inspect.signature(AbstractDbtBase.__init__).parameters.keys()}:
            try:
                base_kwargs[arg] = kwargs[arg]
            except KeyError:
                try:
                    base_kwargs[arg] = default_args[arg]
                except KeyError:
                    pass
        AbstractDbtBase.__init__(self, **base_kwargs)
        BatchOperator.__init__(self, **operator_kwargs)

    def build_and_run_cmd(self,
                          context,
                          cmd_flags,
                          run_as_async: bool = False,
                          async_context: dict[str, Any] | None = None,
                          ) -> Any:
        self.build_command(context, cmd_flags)
        self.log.info(f"Running command: {self.command}")
        result = BatchOperator.execute(self, context)
        logger.info(result)

    def build_command(self, context: Context, cmd_flags: list[str] | None = None) -> None:
        self.dbt_executable_path = "dbt"
        dbt_cmd, env_vars = self.build_cmd(
            context=context, cmd_flags=cmd_flags)
        # self.environment_variables = {**env_vars, **self.environment_variables}
        self.command = dbt_cmd
        self.container_overrides['command'] = self.command


class DbtBuildAwsBatchOperator(DbtBuildMixin, DbtAwsBatchBaseOperator):
    """
    Executes a dbt core build command.
    """

    template_fields: Sequence[str] = DbtAwsBatchBaseOperator.template_fields + \
        DbtBuildMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtLSAwsBatchOperator(DbtLSMixin, DbtAwsBatchBaseOperator):
    """
    Executes a dbt core ls command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSeedAwsBatchOperator(DbtSeedMixin, DbtAwsBatchBaseOperator):
    """
    Executes a dbt core seed command.

    :param full_refresh: dbt optional arg - dbt will treat incremental models as table models
    """

    template_fields: Sequence[str] = DbtAwsBatchBaseOperator.template_fields + \
        DbtSeedMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSnapshotAwsBatchOperator(DbtSnapshotMixin, DbtAwsBatchBaseOperator):
    """
    Executes a dbt core snapshot command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtSourceAwsBatchOperator(DbtSourceMixin, DbtAwsBatchBaseOperator):
    """
    Executes a dbt source freshness command.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtRunAwsBatchOperator(DbtRunMixin, DbtAwsBatchBaseOperator):
    """
    Executes a dbt core run command.
    """

    template_fields: Sequence[str] = DbtAwsBatchBaseOperator.template_fields + \
        DbtRunMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class DbtTestAwsBatchOperator(DbtTestMixin, DbtAwsBatchBaseOperator):
    """
    Executes a dbt core test command.
    """

    def __init__(self, on_warning_callback: Callable[..., Any] | None = None, **kwargs: str) -> None:
        super().__init__(**kwargs)
        # as of now, on_warning_callback in docker executor does nothing
        self.on_warning_callback = on_warning_callback


class DbtRunOperationAwsBatchOperator(DbtRunOperationMixin, DbtAwsBatchBaseOperator):
    """
    Executes a dbt core run-operation command.

    :param macro_name: name of macro to execute
    :param args: Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the
        selected macro.
    """

    template_fields: Sequence[str] = DbtAwsBatchBaseOperator.template_fields + \
        DbtRunOperationMixin.template_fields  # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
