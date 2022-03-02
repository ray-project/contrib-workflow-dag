from abc import ABCMeta, abstractmethod
from typing import Any, Callable
from typing import Union

import ray
from ray import ObjectRef
from ray.remote_function import RemoteFunction
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
class FunctionNode(Node):
    """FunctionNode class.

    FunctionNode leverages workflow's step function to achieve a functional node.
    Input data is passed to the underlying step function while
    the output of the step function is used as node output.
    """
    def __init__(self,
                 func: Callable,
                 name=None,
                 checkpoint=False,
                 ray_options=None,
                 ):
        if ray_options is not None and not isinstance(ray_options, dict):
            raise ValueError("ray_options must be a dict.")

        self._func = func
        self._name = name or get_qualname(func)
        self._checkpoint = checkpoint
        self._ray_options = ray_options or {}
        self._remote_func: RemoteFunction = ray.remote(*self._ray_options)(self._func) if self._ray_options \
            else ray.remote(self._func)

    def get_name(self):
        return self._name

    def execute(self, *args, **kwargs) -> ObjectRef:
        return self._remote_func.remote(*args, **kwargs)

    # def options(self,
    #             name=None,
    #             step_options=None
    #             ):
    #     """This function set how the step function is going to be executed.
    #
    #     Args:
    #         name: The name of this node.
    #         **step_options: All parameters in this fields will be passed
    #             to the underlying step function options.
    #
    #     Returns:
    #         The node itself.
    #     """
    #     return FunctionNode(self._func, name, step_options)

    def __call__(self, *args, **kwargs):
        raise TypeError("Nodes cannot be called directly")
