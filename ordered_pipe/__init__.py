import concurrent.futures



class OrderedPipe:
    def __init__(self, **kwargs):
        self.inputs = kwargs
        self.task_groups = OrderedPipe.parallel_order(kwargs)

    def parallel_order(dependency_graph):
        # Create a dictionary mapping nodes to their in-degrees and dependency set
        in_degree = {node: 0 for node in dependency_graph}
        dependencies = {node: set() for node in dependency_graph}

        # Populate the in-degree and dependencies
        for node, (operation, *operands) in dependency_graph.items():
            for operand in operands:
                if operand in dependency_graph:  # Only consider internal nodes
                    in_degree[node] += 1
                    dependencies[operand].add(node)

        # List to hold the result as a series of steps with parallelizable tasks
        parallel_steps = []

        # Find all nodes with an in-degree of 0 (i.e., no dependencies)
        independent_nodes = [node for node, degree in in_degree.items() if degree == 0]

        # While there are independent nodes, process them
        while independent_nodes:
            # Add to the current step
            parallel_steps.append(independent_nodes)

            # Nodes to check for the next step
            next_step = []

            # For each independent node, reduce the in-degree of its dependent nodes
            for node in independent_nodes:
                for dependent in dependencies[node]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_step.append(dependent)

            # The next step now becomes the independent nodes
            independent_nodes = next_step

        return parallel_steps


    def __call__(self, parallel, **kwargs):
        self.inputs.update(kwargs)
        if parallel:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for tasks in self.task_groups:
                        futures = {}
                        for task in tasks:
                            f, *id_args = self.inputs[task]
                            args = (self.inputs[id_arg] for id_arg in id_args)
                            futures[executor.submit(f, *args)] = task
                        for future in concurrent.futures.as_completed(futures):
                            task = futures[future]
                            output = future.result()
                            self.inputs[task] = output
            return output
        else:
            for tasks in self.task_groups:
                for task in tasks:
                    f, *id_args = self.inputs[task]
                    args = (self.inputs[id_arg] for id_arg in id_args)
                    output = f(*args)
                    self.inputs[task] = output
            return output