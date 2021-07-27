from clearml import Task, Dataset
from pathlib import Path
import pandas as pd
from sklearn.linear_model import LinearRegression

from tempfile import mkdtemp
import joblib


# Connecting ClearML with the current process,
# from here on everything is logged automatically
task = Task.init(project_name="kedro_tutorial_clearml/ds", task_name="train_model")

split_data_task = Task.get_task(
    project_name="kedro_tutorial_clearml/ds", task_name="split_data"
)
X_train = pd.read_csv(
    split_data_task.artifacts["X_train"].get_local_copy(), compression=None
)
y_train = split_data_task.artifacts["y_train"].get()


regressor = LinearRegression()
regressor.fit(X_train, y_train)

# add and upload local file containing our toy dataset
task.upload_artifact("model", artifact_object=regressor)

with_feature = Dataset.create(
    "name does not matter - the task is the feature",
    dataset_project="kedro_tutorial_clearml/ds",
    parent_datasets=[split_data_task.id],
    use_current_task=True,
)  # This boolean is the main point actually!!!

new_folder = with_feature.get_mutable_local_copy(mkdtemp())
print(f"new_folder is:{new_folder}")
# overwrite with new train df (with the added)
joblib.dump(regressor, new_folder + "/model.pkl")
with_feature.sync_folder(new_folder)
with_feature.upload()
with_feature.finalize()
