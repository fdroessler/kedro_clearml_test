# Copyright 2021 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Project hooks."""
from typing import Any, Dict, Iterable, Optional

from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from clearml import Task
from kedro.versioning.journal import _git_sha
from functools import partial

from clearml.automation.controller import PipelineController


class ProjectHooks:
    @hook_impl
    def register_config_loader(
        self, conf_paths: Iterable[str], env: str, extra_params: Dict[str, Any]
    ) -> ConfigLoader:
        return ConfigLoader(conf_paths)

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ) -> DataCatalog:
        return DataCatalog.from_config(
            catalog, credentials, load_versions, save_version, journal
        )


class ClearmlPipelinehook:
    @hook_impl
    def before_pipeline_run(
        self, run_params: Dict[str, Any], pipeline: Pipeline, catalog: DataCatalog
    ) -> None:
        """Hook to be invoked before a pipeline runs.
        Args:
            run_params: The params needed for the given run.
                Should be identical to the data logged by Journal.
                # @fixme: this needs to be modelled explicitly as code, instead of comment
                Schema: {
                    "run_id": str,
                    "project_path": str,
                    "env": str,
                    "kedro_version": str,
                    "tags": Optional[List[str]],
                    "from_nodes": Optional[List[str]],
                    "to_nodes": Optional[List[str]],
                    "node_names": Optional[List[str]],
                    "from_inputs": Optional[List[str]],
                    "load_versions": Optional[List[str]],
                    "pipeline_name": str,
                    "extra_params": Optional[Dict[str, Any]],
                }
            pipeline: The ``Pipeline`` that will be run.
            catalog: The ``DataCatalog`` to be used during the run.
        """
        pass
        # project name should be read from config file
        task = Task.init(
            project_name="kedro_track_nodes",
            task_name="pipeline",
            reuse_last_task_id=False,
        )
        task.add_tags([f"{k}: {v}" for k, v in run_params.items() if v])
        task.add_tags([f"git_sha: {_git_sha(run_params['project_path'])}"])
        kedro_command = _generate_kedro_command(
            tags=run_params["tags"],
            node_names=run_params["node_names"],
            from_nodes=run_params["from_nodes"],
            to_nodes=run_params["to_nodes"],
            from_inputs=run_params["from_inputs"],
            load_versions=run_params["load_versions"],
            pipeline_name=run_params["pipeline_name"],
        )
        task.add_tags([f"kedro_command: {kedro_command}"])
        task.close()


class ClearmlNodeHook:
    def __init__(self):
        self.flatten = False
        self.recursive = True
        self.sep = "."
        self.long_parameters_strategy = "fail"
        self._is_mlflow_enabled = True

    @hook_impl
    def before_node_run(
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: Dict[str, Any],
        is_async: bool,
        run_id: str,
    ) -> None:
        """Hook to be invoked before a node runs.
        This hook logs all the parameters of the nodes in mlflow.
        Args:
            node: The ``Node`` to run.
            catalog: A ``DataCatalog`` containing the node's inputs and outputs.
            inputs: The dictionary of inputs dataset.
            is_async: Whether the node was run in ``async`` mode.
            run_id: The id of the run.
        """
        task = Task.init(
            project_name=f"kedro_track_nodes/{list(node.tags)[0]}",
            task_name=node.name,
            reuse_last_task_id=False,
        )

        # only parameters will be logged. Artifacts must be declared manually in the catalog
        params_inputs = {}
        datasets = []
        for k, v in inputs.items():
            # detect parameters automatically based on kedro reserved names
            if k.startswith("params:"):
                params_inputs[k] = v
            elif k == "parameters":
                params_inputs[k] = v
            else:
                datasets.append(k)

        # dictionary parameters may be flattened for readibility
        if self.flatten:
            params_inputs = flatten_dict(
                d=params_inputs, recursive=self.recursive, sep=self.sep
            )

        task.connect(params_inputs)

    @hook_impl
    def after_node_run(
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: Dict[str, Any],
        is_async: bool,
        run_id: str,
    ) -> None:
        # here we check if we have a ClearML dataset, if that is the case we keep
        # the current task open so that the artifact can be uploaded. In this case
        # the save function of the clearml artifact class will take care of closing the task
        datasets = [getattr(catalog.datasets, out) for out in node.outputs]
        has_clearml_artifact_dataset = any(
            ["clearml" in d.__module__ for d in datasets]
        )
        if not has_clearml_artifact_dataset:
            task = Task.current_task()
            task.close()


def _generate_kedro_command(
    tags, node_names, from_nodes, to_nodes, from_inputs, load_versions, pipeline_name
):
    cmd_list = ["kedro", "run"]
    SEP = "="
    if from_inputs:
        cmd_list.append("--from-inputs" + SEP + ",".join(from_inputs))
    if from_nodes:
        cmd_list.append("--from-nodes" + SEP + ",".join(from_nodes))
    if to_nodes:
        cmd_list.append("--to-nodes" + SEP + ",".join(to_nodes))
    if node_names:
        cmd_list.append("--node" + SEP + ",".join(node_names))
    if pipeline_name:
        cmd_list.append("--pipeline" + SEP + pipeline_name)
    if tags:
        # "tag" is the name of the command, "tags" the value in run_params
        cmd_list.append("--tag" + SEP + ",".join(tags))
    if load_versions:
        # "load_version" is the name of the command, "load_versions" the value in run_params
        formatted_versions = [f"{k}:{v}" for k, v in load_versions.items()]
        cmd_list.append("--load-version" + SEP + ",".join(formatted_versions))

    kedro_cmd = " ".join(cmd_list)
    return kedro_cmd


def flatten_dict(d, recursive: bool = True, sep="."):
    def expand(key, value):
        if isinstance(value, dict):
            new_value = flatten_dict(value) if recursive else value
            return [(key + sep + k, v) for k, v in new_value.items()]
        else:
            return [(key, value)]

    items = [item for k, v in d.items() for item in expand(k, v)]

    return dict(items)
