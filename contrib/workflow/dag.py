from typing import Union

from ray.util.annotations import PublicAPI

from contrib.workflow.node import Node


@PublicAPI(stability="beta")
class DAG:
    """DAG class.
    """
    def __init__(self):
        self._nodes = {}
        self._edges = []
        self._upstreams = {}
        self._downstreams = {}
        self._node_levels = {}
        self._level_nodes = {}
        self._node_output = {}
        self._node_in_args = {}

    def reset(self):
        self._reset()

    def _reset(self):
        self._nodes = {}
        self._edges = []
        self._upstreams = {}
        self._downstreams = {}
        self._node_levels = {}
        self._level_nodes = {}
        self._node_output = {}
        self._node_in_args = {}

    def _reset_levels(self):
        """
        Graph is mutable, so when graph is muted (e.g. adding a new node),
        we need to reset some attributes cache
        """
        self._node_levels = {}
        self._level_nodes = {}

    def execute(self, node=None):
        """Execute the graph, optionally on a target node.

        If node is not None, it will execute on the target node only,
        i.e. all pre-nodes relate to the target node will be executed
        while non-related nodes will not be executed.
        If node is None, it will execute on the last node in the output
        layer. TODO: what would be a better default behavior?

        Example:
            >>> data_input_1 = DataNode("input1", 10)
            >>> data_input_2 = DataNode("input2", 20)
            >>> data_input_3 = DataNode("input3", 30)

            >>> @workflow.node
            ... def minus(left: int, right: int) -> int:
            ...     left - right

            >>> @workflow.node
            ... def multiply(a: int, b: int) -> int:
            ...     return a * b

            >>> graph = DAG()
            >>> graph.add_edge(data_input_1, minus, 0)
            >>> graph.add_edge(data_input_2, minus, 1)
            >>> graph.add_edge(minus, multiply, 0)
            >>> graph.add_edge(data_input_3, multiply, 1)
            >>> result = graph.execute()

        Args:
            node: Target node to execute on.

        Returns:

        """
        return self._execute(node)

    def _execute(self, node=None):
        nodes_by_level = self.get_nodes_by_level()
        final = None
        for level in nodes_by_level:
            for _node in nodes_by_level[level]:
                final = self._execute_node(_node)
        if node is not None:
            return self._node_output[node].run()
        else:
            return final.run()

    def _execute_node(self, node: Node):
        """
        lazy execution
        This will populate the step function result to self._node_output, skipped if already populated
        """
        if node in self._node_output:
            return self._node_output[node]
        args_pos_and_val = []
        kwargs = {}
        for pre_node in self.get_pre_nodes(node):
            mapping = self._node_in_args[node][pre_node]
            value = self._node_output[pre_node]
            if isinstance(mapping, int):
                args_pos_and_val.append([mapping, value])
            else:
                kwargs[mapping] = value
        args_pos_and_val.sort(key=lambda l: l[0])
        args_pos = [item[0] for item in args_pos_and_val]
        assert args_pos == list(range(len(args_pos_and_val))), \
            "Node {} has incorrect incoming positional args: {}".format(node.get_name(), args_pos)
        args = [arg[1] for arg in args_pos_and_val]
        result = node.execute(*args, **kwargs)
        self._node_output[node] = result
        return result

    def add_node(self, node: Node):
        self._reset_levels()
        if node not in self._upstreams:
            self._upstreams[node] = []
        if node not in self._downstreams:
            self._downstreams[node] = []
        if node not in self._nodes:
            self._nodes[node.get_name()] = node

    def _add_node_in_args(self, from_node, to_node, arg):
        if to_node not in self._node_in_args:
            self._node_in_args[to_node] = {}
        self._node_in_args[to_node][from_node] = arg

    def add_edge(self, from_node: Node, to_node: Node, arg_mapping: Union[int, str] = 0):
        """Adds an edge to the graph.

        This adds an edge from from_node to to_node, with from_node and to_node added
        to the graph as well if they don't exist.
        The output of from_node will be used as part of the input for to_node, with
        arg_mapping as argument identifier. arg_mapping can be either positional (int)
        or kwargs (str). For example: add_edge(node1, node2, 0)
        means the output of node2 will become the first argument of node1's input;
        add_edge(node3, node4, 3) means the output of node3 will become the 4th
        positional argument of node4's input; and add_edge(node5, node6, "a")
        means the output of node5 will become the kwarg "a" of node6's input.

        Example:
            >>> data_input_1 = DataNode("input1", 10)
            >>> data_input_2 = DataNode("input2", 20)
            >>> data_input_3 = DataNode("input3", 30)

            >>> @workflow.node
            ... def minus(left: int, right: int) -> int:
            ...     left - right

            >>> @workflow.node
            ... def multiply(a: int, b: int) -> int:
            ...     return a * b

            >>> # Use positional args
            >>> graph = DAG()
            >>> graph.add_edge(data_input_1, minus, 0)
            >>> graph.add_edge(data_input_2, minus, 1)
            >>> graph.add_edge(minus, multiply, 0)
            >>> graph.add_edge(data_input_3, multiply, 1)
            >>> result = graph.execute()

            >>> # Use kwargs
            >>> graph = DAG()
            >>> graph.add_edge(data_input_1, minus, "left")
            >>> graph.add_edge(data_input_2, minus, "right")
            >>> graph.add_edge(minus, multiply, "a")
            >>> graph.add_edge(data_input_3, multiply, "b")
            >>> result = graph.execute()


        Args:
            from_node: In node.
            to_node: Out node.
            arg_mapping: The input argument for to_node when using output from from_node.
            It can be either positional (int) or kwargs (str). default is 0.

        Returns:

        """
        self.add_node(from_node)
        self.add_node(to_node)

        self._edges.append([from_node, to_node])
        self._upstreams[to_node].append(from_node)
        self._downstreams[from_node].append(to_node)

        self._add_node_in_args(from_node, to_node, arg_mapping)

    def get_edges(self):
        return self._edges

    # # TODO: find a better way for plotting the graph
    # def plot(self):
    #     try:
    #         import pydot
    #     except ImportError:
    #         raise ValueError("pydot is required to plot DAG")
    #     import tempfile
    #     graph = pydot.Dot(rankdir="LR")
    #
    #     # this section is for aligning only, we need to make sure nodes
    #     # on the same level are aligned vertically in the plot
    #     nodes_by_level = self.get_nodes_by_level()
    #     for level in nodes_by_level:
    #         subgraph = pydot.Subgraph(rank="same")
    #         for node in nodes_by_level[level]:
    #             subgraph.add_node(pydot.Node(node.get_name()))
    #         graph.add_subgraph(subgraph)
    #
    #     for edge in self.get_edges():
    #         graph.add_edge(pydot.Edge(edge[0].get_name(), edge[1].get_name()))
    #
    #     with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
    #         graph.write(tmp.name, format="png")
    #         try:
    #             from IPython import display
    #             return display.Image(filename=tmp.name)
    #         except ImportError:
    #             pass

    def _compute_node_level(self, node: Node, result: dict):
        if node in result:
            return result[node]

        pre_nodes = self.get_pre_nodes(node)
        if not pre_nodes:
            result[node] = 0
            return 0

        max_level = 0
        for p_node in pre_nodes:
            level = self._compute_node_level(p_node, result)
            max_level = max(level, max_level)

        result[node] = max_level + 1

        return max_level + 1

    def _compute_node_levels(self):
        if self._node_levels:
            return self._node_levels

        for node in self._upstreams:
            self._node_levels[node] = self._compute_node_level(node, self._node_levels)

        return self._node_levels

    def get_node_levels(self):
        self._compute_node_levels()
        return self._node_levels

    def get_node_level(self, node: Node):
        self._compute_node_levels()
        return self._node_levels[node]

    def get_max_level(self):
        levels = self._compute_node_levels()
        max_level = 0
        for node, node_level in levels.items():
            max_level = max(node_level, max_level)
        return max_level

    def get_nodes_by_level(self):
        if self._level_nodes:
            return self._level_nodes

        levels = self._compute_node_levels()
        for node, node_level in levels.items():
            if node_level not in self._level_nodes:
                self._level_nodes[node_level] = []
            self._level_nodes[node_level].append(node)

        return self._level_nodes

    def get_post_nodes(self, node: Node):
        """Get all nodes that flows out from the target node, i.e. downstream nodes"""
        return self._downstreams[node]

    def get_pre_nodes(self, node: Node):
        """Get all nodes that flows into the target node, i.e. upstream dependencies"""
        return self._upstreams[node]

    def is_output(self, node: Node):
        post_nodes = self.get_post_nodes(node)
        return not post_nodes

    def get_output_nodes(self):
        # dict from level to nodes
        terminal_nodes = []
        for node in self._upstreams.keys():
            if self.is_output(node):
                terminal_nodes.append(node)
        return terminal_nodes

    def get_nodes(self):
        return self._nodes

    def is_input(self, node: Node):
        pre_nodes = self.get_pre_nodes(node)
        return not pre_nodes

    def get_input_nodes(self):
        input_nodes = []
        for node in self._nodes.values():
            if self.get_node_level(node) == 0:
                input_nodes.append(node)

        return input_nodes

    def get_node(self, node_name: str) -> Node:
        return self._nodes[node_name]

    def get_node_output(self, node: Node):
        return self._node_output[node]
