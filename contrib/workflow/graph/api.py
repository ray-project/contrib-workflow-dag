from typing import Dict, Any

from ray.util.annotations import PublicAPI

from contrib.workflow.graph.node import FunctionNode


def make_node_decorator(node_options: Dict[str, Any]):
    def decorator(func):
        return FunctionNode(func, **node_options)

    return decorator


# @PublicAPI(stability="alpha")
def node(*args, **kwargs):
    """A decorator used for creating workflow nodes.
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return make_node_decorator({})(args[0])
    if len(args) != 0:
        raise ValueError(f"Invalid arguments for node decorator {args}")
    node_options = {}
    name = kwargs.pop("name", None)
    if name is not None:
        node_options["name"] = name
    if len(kwargs) != 0:
        node_options["step_options"] = kwargs
    return make_node_decorator(node_options)

