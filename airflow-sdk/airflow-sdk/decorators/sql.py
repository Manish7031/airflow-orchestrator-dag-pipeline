## custom decocrator for sql tasks implmenentation
from typing import Sequence, ClassVar, Collection, Any, Mapping, Callable
from airflow.sdk.bases.decorator import DecoratedOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.definitions._internal.types import Collection, Mapping, Any, SET_DURING_EXECUTION
from collections.abc import Sequence
from airflow.utils.context import context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.sdk.definitions.context import Context
from airflow.sdk.bases.decorator import task_decorator_factory, TaskDecorator
import warnings
class _SQLDecoratedOperator(DecoratedOperator, SQLExecuteQueryOperator):
    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *SQLExecuteQueryOperator.template_fields)
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **SQLExecuteQueryOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.sql"
    overwrite_rtif_after_execution:bool = True
    
    def __init__(
        self,
        *,
        python_callable: callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) ->None:
        if kwargs.pop("multiple-outputs", None):
            Warning.warn(
                f"multile-outputs is not supported for {self.custom_operator_name} tasks. Ignoring the argument."
                UserWarning,
                stacklevel=3,
         )
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            sql=SET_DURING_EXECUTION,
            multiple_outputs=False,
            **kwargs,
        )
            
    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args context)
        self.sql = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(self.sql, str):
            raise TypeError(f"expected return value callable must be non empty)")

        context["ti"].render_templates()
        return super().execute(context)

def sql_task(python_callable: callable | None: None, *kwargs) -> TaskDecorator:return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_SQLDecoratedOperator,
        **kwargs)

# @task.sql
# def my_task():
#     return "SELECT COUNT(*) FROM xcom"
