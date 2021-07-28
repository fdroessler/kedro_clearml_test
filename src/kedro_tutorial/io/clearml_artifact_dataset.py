from typing import Any, Dict, Union
from pathlib import Path

from kedro.io import AbstractVersionedDataSet
from kedro.io.core import parse_dataset_definition
from clearml import Task


class ClearMLArtifactDataSet(AbstractVersionedDataSet):
    """This class is a wrapper for any kedro AbstractDataSet.
    It decorates their ``save`` method to log the dataset in clearml when ``save`` is called.

    """

    def __new__(
        cls,
        data_set: Union[str, Dict],
        run_id: str = None,
        artifact_path: str = None,
        credentials: Dict[str, Any] = None,
    ):

        data_set, data_set_args = parse_dataset_definition(config=data_set)

        # fake inheritance : this clearml class should be a mother class which wraps
        # all dataset (i.e. it should replace AbstractVersionedDataSet)
        # instead and since we can't modify the core package,
        # we create a subclass which inherits dynamically from the data_set class
        class ClearMLArtifactDataSetChildren(data_set):
            def __init__(self, run_id, artifact_path):
                super().__init__(**data_set_args)
                self.run_id = run_id
                self.artifact_path = artifact_path
                self._logging_activated = True

            @property
            def _logging_activated(self):
                return self.__logging_activated

            @_logging_activated.setter
            def _logging_activated(self, flag):
                if not isinstance(flag, bool):
                    raise ValueError(
                        f"_logging_activated must be a boolean, got {type(flag)}"
                    )
                self.__logging_activated = flag

            def _save(self, data: Any):
                # _get_save_path needs to be called before super, otherwise
                # it will throw exception that file under path already exist.
                local_path = (
                    self._get_save_path()
                    if hasattr(self, "_version")
                    else self._filepath
                )
                # it must be converted to a string with as_posix()
                # for logging on remote storage like Azure S3
                local_path = local_path.as_posix()

                super()._save(data)
                if self._logging_activated:
                    if self.run_id:
                        # if a run id is specified, we have to use mlflow client
                        # to avoid potential conflicts with an already active run
                        tasks = Task.get_tasks(
                            project_name="kedro_test",
                            task_filter={
                                "tags": [f"run_id: {self.run_id}"],
                            },
                        )
                        if len(tasks) > 1:
                            raise ValueError(
                                "There should not be more than one run with the same run_id"
                            )
                        # TODO: This case should be captured before as expensive calculations otherwise fail here.
                        elif len(tasks) == 0:
                            raise ValueError(
                                "There is no run with this run_id, don't know where to log the result"
                            )
                        else:
                            upload_task = tasks[0]
                        upload_task.upload_artifact(
                            Path(local_path).name, local_path, wait_on_upload=True
                        )
                        upload_task.close()
                        # Also close the current task
                        task = Task.current_task()
                        task.close()
                    else:
                        task = Task.current_task()
                        # TODO: If it is a "viewable" object upload it directly otherwise upload the saved file
                        task.upload_artifact(Path(local_path).name, local_path)
                        task.upload_artifact(f"rep_{Path(local_path).stem}", data)

        # rename the class
        parent_name = data_set.__name__
        ClearMLArtifactDataSetChildren.__name__ = f"ClearML{parent_name}"
        ClearMLArtifactDataSetChildren.__qualname__ = (
            f"{parent_name}.ClearML{parent_name}"
        )

        clearml_dataset_instance = ClearMLArtifactDataSetChildren(
            run_id=run_id, artifact_path=artifact_path
        )
        return clearml_dataset_instance

    def _load(self) -> Any:  # pragma: no cover
        """
        MlflowArtifactDataSet is a factory for DataSet
        and consequently does not implements abtracts methods
        """
        pass

    def _save(self, data: Any) -> None:  # pragma: no cover
        """
        MlflowArtifactDataSet is a factory for DataSet
        and consequently does not implements abtracts methods
        """
        pass

    def _describe(self) -> Dict[str, Any]:  # pragma: no cover
        """
        MlflowArtifactDataSet is a factory for DataSet
        and consequently does not implements abtracts methods
        """
        pass
