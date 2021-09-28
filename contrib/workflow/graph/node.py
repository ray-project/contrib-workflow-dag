from abc import ABCMeta, abstractmethod
from typing import Any, Callable
from typing import Union

from ray import ObjectRef
from ray.util.annotations import PublicAPI
from ray.workflow.common import get_module, get_qualname, Workflow
from ray.workflow.step_function import WorkflowStepFunction


@PublicAPI(stability="beta")
class Node(metaclass=ABCMeta):
    """Abstract base class for Node, this provides a Node interface.
    """
    @abstractmethod
    def get_name(self) -> str:
        """Get name of the node"""

    @abstractmethod
    def execute(self, *args, **kwargs) -> Union[Workflow, ObjectRef, Any]:
        """A lazy-evaluation callable"""


@PublicAPI(stability="beta")
class DataNode(Node):
    """DataNode class.

    DataNode contains only value (either object or ray.ObjectRef),
    and is typically used as data input for the graph.
    """
    def __init__(self, name, value):
        self._name = name
        self._value = value

    def get_name(self):
        return self._name

    def execute(self):
        return self._value


@PublicAPI(stability="beta")
class FunctionNode(Node):
    """FunctionNode class.

    FunctionNode leverages workflow's step function to achieve a functional node.
    Input data is passed to the underlying step function while
    the output of the step function is used as node output.
    """
    def __init__(self,
                 func: Union[Callable, WorkflowStepFunction],
                 name=None,
                 step_options=None):
        if step_options is not None and not isinstance(step_options, dict):
            raise ValueError("step_options must be a dict.")

        self._func = func
        self._step_options = step_options or {}

        if isinstance(func, WorkflowStepFunction):
            self._step_func = self._func.options(**self._step_options)
            self._name = name or self._step_func.step.__name__

        else:
            self._step_func = WorkflowStepFunction(self._func, **self._step_options)
            self._name = name or get_module(func) + "." + get_qualname(func)

    def get_name(self):
        return self._name

    def execute(self, *args, **kwargs):
        return self._step_func.step(*args, **kwargs)

    def options(self,
                name=None,
                step_options=None
                ):
        """This function set how the step function is going to be executed.

        Args:
            name: The name of this node.
            **step_options: All parameters in this fields will be passed
                to the underlying step function options.

        Returns:
            The node itself.
        """
        return FunctionNode(self._func, name, step_options)

    def __call__(self, *args, **kwargs):
        raise TypeError("Workflow nodes cannot be called directly")
