# <project_root>/register_prefect_flow.py
from pathlib import Path

import click

from clearml import Task
from pandas.core.algorithms import isin
from prefect.utilities.exceptions import ClientError

from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline.node import Node
from kedro.runner import run_node
from functools import partial
import pickle
from clearml.automation.controller import PipelineController
import pandas as pd

PROJECT_NAME = "kedro_clearml_pipeline"


def new_func(f, datasets, parameters, outputs, inputs):
    input_to_arg_mapping = dict(
        zip(
            inputs,
            f.__code__.co_varnames[: f.__code__.co_argcount],
        )
    )
    kwargs = {}
    for name in datasets:
        dataset_task = Task.get_tasks(
            project_name=PROJECT_NAME,
            task_filter={"tags": [name]},
        )[-1]
        kwargs[input_to_arg_mapping[name]] = pickle.loads(
            dataset_task.artifacts[name].get()
        )
        dataset_task.close()
        if isinstance(kwargs[input_to_arg_mapping[name]], pd.DataFrame):
            task = Task.current_task()
            task.upload_artifact(
                f"input_{name}", kwargs[input_to_arg_mapping[name]], wait_on_upload=True
            )
            task.close()
    kwargs = {**kwargs, **parameters}
    results = f(**kwargs)
    results = dict(zip(outputs, results))
    task = Task.current_task()
    for name, res in results.items():
        task.add_tags(name)
        task.upload_artifact(f"{name}_df", res, wait_on_upload=True)
        task.upload_artifact(name, pickle.dumps(res), wait_on_upload=True)


def upload_data_for_test(datasets, catalog):
    task_upload = Task.init(
        project_name=f"{PROJECT_NAME}", task_name="raw_data", tags=datasets
    )
    if (task_upload is None) or (task_upload.name != "raw_data"):
        return
    for data in datasets:
        task_upload.upload_artifact(
            f"{data}", pickle.dumps(catalog.load(data)), wait_on_upload=True
        )
    task_upload.close()


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

    raw_datasets = pipeline.all_inputs() - pipeline.all_outputs()
    raw_datasets = [
        k
        for k in raw_datasets
        if (
            hasattr(context.io.datasets, k)
            and not isinstance(getattr(context.io.datasets, k), MemoryDataSet)
        )
    ]
    upload_data_for_test(raw_datasets, catalog)

    task = Task.init(
        project_name=PROJECT_NAME,
        task_type=Task.TaskTypes.controller,
        task_name="overview",
        reuse_last_task_id=False,
    )

    if task is not None:
        pipe = PipelineController(
            default_execution_queue="default", add_pipeline_tags=False
        )
    for node, parent_node in pipeline.node_dependencies.items():
        input_to_arg_mapping = dict(
            zip(
                node.inputs,
                node.func.__code__.co_varnames[: node.func.__code__.co_argcount],
            )
        )
        params_inputs = {}
        datasets = []
        for inp in node.inputs:
            # detect parameters automatically based on kedro reserved names
            if inp.startswith("params:"):
                params_inputs[input_to_arg_mapping[inp]] = catalog.load(inp)
            elif inp == "parameters":
                params_inputs[input_to_arg_mapping[inp]] = catalog.load(inp)
            else:
                datasets.append(inp)
        func_to_run = partial(new_func, node.func)
        task_name = task.create_function_task(
            func_to_run,
            node.name,
            datasets=datasets,
            parameters=params_inputs,
            outputs=node.outputs,
            inputs=node.inputs,
        )
        if task_name is not None:
            pipe.add_step(
                name=node.name,
                base_task_project=PROJECT_NAME,
                base_task_id=task_name.id,
                parents=[vv.name for vv in parent_node],
            )
    if task is not None:
        # Starting the pipeline (in the background)
        pipe.start()
        # Wait until pipeline terminates
        pipe.wait()
        # cleanup everything
        pipe.stop()

        print("done")

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
