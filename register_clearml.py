from pathlib import Path
from typing import List

import click

from clearml import Task

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
import datetime
from clearml import PipelineController

PROJECT_NAME = "kedro_clearml_pipeline"
TIMESTAMP = "{:%d-%m-%Y_%H:%M:%S}".format(datetime.datetime.now())


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
    if not isinstance(results, (list, tuple)):
        results = [results]
    results = dict(zip(outputs, results))
    task = Task.current_task()
    for name, res in results.items():
        task.add_tags(name)
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
    raw_data_urls = {k: v.url for k, v in task_upload.artifacts.items()}
    task_upload.close()
    return raw_data_urls


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
    raw_data_urls = upload_data_for_test(raw_datasets, catalog)

    # task = Task.init(
    #     project_name=PROJECT_NAME,
    #     task_type=Task.TaskTypes.controller,
    #     task_name="overview_" + TIMESTAMP,
    #     reuse_last_task_id=False,
    # )

    # if task is not None:
    #     pipe = PipelineController(
    #         default_execution_queue="gpu_support",
    #         add_pipeline_tags=False,
    #     )

    # create the pipeline controller
    pipe = PipelineController(
        project=PROJECT_NAME,
        name="pipeline demo",
        version="1.1",
        add_pipeline_tags=False,
    )
    pipe.set_default_execution_queue("default")
    for k, v in raw_data_urls.items():
        pipe.add_parameter(name=k, description="url to pickle file", default=v)
    for group in pipeline.grouped_nodes:
        for node in group:
            parent_nodes = pipeline.node_dependencies[node]
            input_to_arg_mapping = dict(
                zip(
                    node.inputs,
                    node.func.__code__.co_varnames[: node.func.__code__.co_argcount],
                )
            )
            params_inputs = {}
            for inp in node.inputs:
                # detect parameters automatically based on kedro reserved names
                if inp.startswith("params:"):
                    params_inputs[input_to_arg_mapping[inp]] = catalog.load(inp)
                elif inp == "parameters":
                    params_inputs[input_to_arg_mapping[inp]] = catalog.load(inp)
                else:
                    if inp in raw_data_urls.keys():
                        params_inputs[input_to_arg_mapping[inp]] = raw_data_urls[inp]
                    else:
                        for parent_node in parent_nodes:
                            if inp in parent_node.outputs:
                                params_inputs[
                                    input_to_arg_mapping[inp]
                                ] = f"${{{parent_node.name}.{inp}}}"
            # func_to_run = partial(new_func, node.func)
            pipe.add_function_step(
                name=node.name,
                function=node.func,
                function_kwargs=params_inputs,
                function_return=node.outputs
                if isinstance(node.outputs, List)
                else list(node.outputs),
                cache_executed_step=True,
            )
        # pipe.add_function_step(
        #     name="step_two",
        #     # parents=['step_one'],  # the pipeline will automatically detect the dependencies based on the kwargs inputs
        #     function=step_two,
        #     function_kwargs=dict(data_frame="${step_one.data_frame}"),
        #     function_return=node.outputs,
        #     cache_executed_step=True,
        # )
        # task_name = task.create_function_task(
        #     func_to_run,
        #     node.name,
        #     node.name,
        #     datasets=datasets,
        #     parameters=params_inputs,
        #     outputs=node.outputs,
        #     inputs=node.inputs,
        # )
        # if task_name is not None:
        #     task_name.add_tags([TIMESTAMP])
        #     pipe.add_step(
        #         name=node.name,
        #         base_task_project=PROJECT_NAME,
        #         base_task_id=task_name.id,
        #         parents=[vv.name for vv in parent_node],
        #     )
    # Starting the pipeline (in the background)
    pipe.start()
    # Wait until pipeline terminates
    pipe.wait()
    # cleanup everything
    pipe.stop()
    print("done")


if __name__ == "__main__":
    build_and_register_flow()
