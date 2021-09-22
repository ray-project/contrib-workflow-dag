from typing import Dict, Any, Callable

from ray.workflow.common import get_module, get_qualname
from ray.workflow.step_function import WorkflowStepFunction

from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class Node:
    """
    Node class
    """
    def __init__(self,
                 func: Callable,
                 name=None,
                 step_options=None):
        if step_options is not None and not isinstance(step_options, dict):
            raise ValueError("step_options must be a dict.")

        self._func = func
        self._name = name or get_module(func) + "." + get_qualname(func)
        self._step_options = step_options or {}

        self._step_func = WorkflowStepFunction(self._func, **self._step_options)

        self.step = self._step_func.step

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
        return Node(self._func, name, step_options)

    def __call__(self, *args, **kwargs):
        raise TypeError("Workflow nodes cannot be called directly")

    def get_name(self):
        return self._name

