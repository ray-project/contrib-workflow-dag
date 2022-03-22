from typing import Union

from ray.util.annotations import PublicAPI

from contrib.workflow.graph.node import Node


@PublicAPI(stability="beta")
class DAG:
    """DAG class.
    """
    def __init__(self):
        self._nodes = []
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

    def execute(self, data=None, node=None):
        """Execute the graph, optionally on a target node.

        Data is passed to the graph with "data" argument in the form of
        a dictionary {nodeA: inputA; nodeB: inputB, ...}.
        Each input is another dictionary containing the data passed
        to the particular node inputA: {k1: v1, k2: v2, ...}, where
        keys can be either int (for positional arg) or str (for kwargs).

        If node is not None, it will execute on the target node only,
        i.e. all pre-nodes relate to the target node will be executed
        while non-related nodes will not be executed.
        If node is None, it will execute on the last node in the output
        layer. TODO: what would be a better default behavior?

        Example:

            data_input_1----------↓
                                minus----------↓
            data_input_2----------↑            ↓
                                            multiply
            data_input_3-----------------------↑

            >>> @graph.node
            ... def minus(left, right):
            ...     left - right

            >>> @graph.node
            ... def multiply(a, b):
            ...     return a * b

            >>> dag = DAG()
            >>> dag.add_edge(minus, multiply, "a")
            >>> dag.execute(data={
            ...     minus: {
            ...         "left": 10,
            ...         "right": 20
            ...     },
            ...     multiply: {
            ...         "b": 30
            ...     }
            ... })

        Args:
            data: data to be passed to the graph.
            node: Target node to execute on.

        Returns:

        """
        if not node:
            output_nodes = self.get_output_nodes()
            if len(output_nodes) == 1:
                node = output_nodes[0]
            else:
                raise ValueError("There are more than 1 output nodes in this DAG, "
                                 "please specify the node you want to execute.")
        return self._execute(data, node)

    def _execute(self, data, node):
        data = data or {}
        to_run_nodes = self.get_ancestors(node, True)
        nodes_by_level = self.get_nodes_by_level()
        for level in nodes_by_level:
            for _node in nodes_by_level[level]:
                if _node in to_run_nodes:
                    self._execute_node(_node, data.get(_node, {}))
        return self._node_output[node].run()

    def _execute_node(self, node: Node, arg_vals=None):
        """
        lazy execution
        This will populate the step function result to self._node_output, skipped if already populated.

        Each node takes input from two places: upstream nodes (outputs from in_node) and user inputs
        (data passed to dag.execute()), so we need to gather and combine both and feed into the current
        node.
        """

        args_pos_and_val = []
        kwargs = {}
        # Get input from upstream nodes
        for pre_node in self.get_upstreams(node):
            mapping = self._node_in_args[node][pre_node]
            value = self._node_output[pre_node]
            if isinstance(mapping, int):
                args_pos_and_val.append([mapping, value])
            else:
                kwargs[mapping] = value
        # Get input from user inputs
        args_vals = arg_vals or {}
        for k, v in args_vals.items():
            if isinstance(k, int):
                args_pos_and_val.append([k, v])
            else:
                kwargs[k] = v
        # validate and sort args
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
            self._nodes.append(node)

    def add_nodes(self, nodes):
        for node in nodes:
            self.add_node(node)

    def _add_node_in_args(self, from_node, to_node, arg):
        if to_node not in self._node_in_args:
            self._node_in_args[to_node] = {}
        self._node_in_args[to_node][from_node] = arg

    @classmethod
    def sequential(cls, nodes):
        dag = cls()
        if len(nodes) == 1:
            dag.add_node(nodes[0])
        else:
            for i in range(len(nodes) - 1):
                dag.add_edge(nodes[i], nodes[i+1])
        return dag

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
            >>> @graph.node
            ... def minus(left: int, right: int) -> int:
            ...     left - right

            >>> @graph.node
            ... def multiply(a: int, b: int) -> int:
            ...     return a * b

            >>> # Use positional args
            >>> dag = DAG()
            >>> dag.add_edge(minus, multiply, 0)
            >>> dag.execute(data={
            ...     minus: {
            ...         0: 10,
            ...         1: 20
            ...     },
            ...     multiply: {
            ...         1: 30
            ...     }
            ... })

            >>> # Use kwargs
            >>> dag = DAG()
            >>> dag.add_edge(minus, multiply, "a")
            >>> dag.execute(data={
            ...     minus: {
            ...         "left": 10,
            ...         "right": 20
            ...     },
            ...     multiply: {
            ...         "b": 30
            ...     }
            ... })

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

    def add_edges(self, edges):
        for edge in edges:
            self.add_edge(*edge)

    def get_edges(self):
        return self._edges

    # TODO: internal use only for now until we find a better visualizing
    def _plot(self):
        try:
            import pydot
        except ImportError:
            raise ValueError("pydot is required to plot DAG")
        import tempfile
        graph = pydot.Dot(rankdir="LR")

        # this section is for aligning only, we need to make sure nodes
        # on the same level are aligned vertically in the plot
        nodes_by_level = self.get_nodes_by_level()
        for level in nodes_by_level:
            subgraph = pydot.Subgraph(rank="same")
            for node in nodes_by_level[level]:
                subgraph.add_node(pydot.Node(node.get_name()))
            graph.add_subgraph(subgraph)

        for edge in self.get_edges():
            graph.add_edge(pydot.Edge(edge[0].get_name(), edge[1].get_name()))

        with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
            graph.write(tmp.name, format="png")
            try:
                from IPython import display
                return display.Image(filename=tmp.name)
            except ImportError:
                pass

    def get_ancestors(self, node, including_self=False):
        def dfs(_node, _res):
            if _node not in _res:
                _res.append(_node)
                for _upstream in self._upstreams[_node]:
                    dfs(_upstream, _res)
        res = []
        dfs(node, res)
        if not including_self:
            res.remove(node)
        return res

    def get_descendants(self, node, including_self=False):
        def dfs(_node, _res):
            if _node not in _res:
                _res.append(_node)
                for _downstream in self._downstreams[_node]:
                    dfs(_downstream, _res)
        res = []
        dfs(node, res)
        if not including_self:
            res.remove(node)
        return res

    def _dfs(self, node, res):
        if node in res:
            return res[node]
        if not self._upstreams[node]:
            res[node] = 0
            return 0
        res[node] = max(self._dfs(_node, res) for _node in self._upstreams[node]) + 1
        return res[node]

    def _populate_node_levels(self):
        if self._node_levels:
            return
        res = {}
        for node in self._nodes:
            self._dfs(node, res)
        self._node_levels = res

    def get_node_levels(self):
        self._populate_node_levels()
        return self._node_levels

    def get_node_level(self, node: Node):
        return self.get_node_levels()[node]

    def get_max_level(self):
        levels = self.get_node_levels().values()
        return max(levels) if levels else 0

    def _populate_nodes_by_level(self):
        if self._level_nodes:
            return
        res = {}
        for node, level in self.get_node_levels().items():
            if level not in res:
                res[level] = []
            res[level].append(node)
        self._level_nodes = res

    def get_nodes_by_level(self):
        self._populate_nodes_by_level()
        return self._level_nodes

    def get_downstreams(self, node: Node):
        return self._downstreams[node]

    def get_upstreams(self, node: Node):
        return self._upstreams[node]

    def is_output(self, node: Node):
        downstreams = self.get_downstreams(node)
        return not downstreams

    def get_output_nodes(self):
        output_nodes = []
        for node in self._nodes:
            if self.is_output(node):
                output_nodes.append(node)
        return output_nodes

    def get_nodes(self):
        return self._nodes

    def is_input(self, node: Node):
        upstreams = self.get_upstreams(node)
        return not upstreams

    def get_input_nodes(self):
        input_nodes = []
        for node in self._nodes:
            if self.is_input(node):
                input_nodes.append(node)
        return input_nodes

    def get_node_output(self, node: Node):
        return self._node_output[node]
