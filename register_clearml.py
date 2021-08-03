# <project_root>/register_prefect_flow.py
from pathlib import Path

import click

from clearml import Task
from prefect.utilities.exceptions import ClientError

from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline.node import Node
from kedro.runner import run_node
from functools import partial

task = Task.init(
    project_name="kedro_track_with_data",
    task_name="overview",
    reuse_last_task_id=False,
)


def new_func(f, datasets, parameters):
    kwargs = {}
    for name in datasets:
        dataset_task = Task.get_task(
            project_name="kedro_track_nodes/DE Pipeline",
            task_name="create_model_input_table_node",
        )
        kwargs[name] = dataset_task.artifacts["rep_model_input_table"].get()
    kwargs = {**kwargs, **parameters}
    return f(**kwargs)


@click.command()
@click.option("-p", "--pipeline", "pipeline_name", default=None)
@click.option("--env", "-e", type=str, default=None)
def build_and_register_flow(pipeline_name, env):
    """Register a Kedro pipeline as a Prefect flow."""
    project_path = Path.cwd()
    metadata = bootstrap_project(project_path)

    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    catalog = context.catalog
    pipeline_name = pipeline_name or "__default__"
    pipeline = pipelines.get(pipeline_name)

    unregistered_ds = pipeline.data_sets() - set(catalog.list())
    for ds_name in unregistered_ds:
        catalog.add(ds_name, MemoryDataSet())

    tasks = {}
    for node in pipeline.nodes:
        if node.name == "split_data_node":
            params_inputs = {}
            datasets = []
            for inp in node.inputs:
                # detect parameters automatically based on kedro reserved names
                if inp.startswith("params:"):
                    params_inputs[inp] = catalog.load(inp)
                elif inp == "parameters":
                    params_inputs[inp] = catalog.load(inp)
                else:
                    datasets.append(inp)
            func_to_run = partial(new_func, node.func)
            task.create_function_task(func_to_run, node.name, **params_inputs)

    # for node, parent_nodes in pipeline.node_dependencies.items():
    #     if node._unique_key not in tasks:
    #         node_task = KedroTask(node, catalog)
    #         tasks[node._unique_key] = node_task
    #     else:
    #         node_task = tasks[node._unique_key]

    #     parent_tasks = []

    #     for parent in parent_nodes:
    #         if parent._unique_key not in tasks:
    #             parent_task = KedroTask(parent, catalog)
    #             tasks[parent._unique_key] = parent_task
    #         else:
    #             parent_task = tasks[parent._unique_key]

    #         parent_tasks.append(parent_task)

    #     flow.set_dependencies(task=node_task, upstream_tasks=parent_tasks)


if __name__ == "__main__":
    build_and_register_flow()
