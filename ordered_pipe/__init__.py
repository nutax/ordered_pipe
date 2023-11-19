import concurrent.futures

class OrderedPipe:
    def __init__(self, **kwargs):
        self.inputs = kwargs
        self.task_groups = OrderedPipe.parallel_order(kwargs)

    def parallel_order(dependency_graph):
        in_degree = {node: 0 for node in dependency_graph}
        dependencies = {node: set() for node in dependency_graph}

        for node, (operation, *operands) in dependency_graph.items():
            for operand in operands:
                if operand in dependency_graph:
                    in_degree[node] += 1
                    dependencies[operand].add(node)

        parallel_steps = []

        independent_nodes = [node for node, degree in in_degree.items() if degree == 0]

        while independent_nodes:
            parallel_steps.append(independent_nodes)

            next_step = []

            for node in independent_nodes:
                for dependent in dependencies[node]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_step.append(dependent)

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