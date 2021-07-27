from clearml import Task, Dataset
from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split
from tempfile import mkdtemp


# Connecting ClearML with the current process,
# from here on everything is logged automatically
task = Task.init(project_name="kedro_tutorial_clearml/ds", task_name="split_data")

parameters = {
    "features": [
        "engines",
        "passenger_capacity",
        "d_check_complete",
        "moon_clearance_complete",
        "iata_approved",
        "company_rating",
        "review_scores_rating",
    ],
    "random_state": 42,
    "test_size": 0.2,
}

task.connect(parameters)

model_input_task = Task.get_task(
    project_name="kedro_tutorial_clearml/de", task_name="create_model_input_table"
)

model_input = pd.read_csv(model_input_task.artifacts["data"].get())

X = model_input[parameters["features"]]
y = model_input["price"]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=parameters["test_size"], random_state=parameters["random_state"]
)

# add and upload local file containing our toy dataset
task.upload_artifact("X_train", artifact_object=X_train)
task.upload_artifact("X_test", artifact_object=X_test)
task.upload_artifact("y_train", artifact_object=y_train)
task.upload_artifact("y_test", artifact_object=y_test)

with_feature = Dataset.create(
    "name does not matter - the task is the feature",
    dataset_project="kedro_tutorial_clearml/ds",
    parent_datasets=[model_input_task.id],
    use_current_task=True,
)  # This boolean is the main point actually!!!

new_folder = with_feature.get_mutable_local_copy(mkdtemp())
print(f"new_folder is:{new_folder}")
# overwrite with new train df (with the added)
X_train.to_pickle(new_folder + "/X_train.pk")
X_test.to_pickle(new_folder + "/X_test.pk")
y_train.to_pickle(new_folder + "/y_train.pk")
y_test.to_pickle(new_folder + "/y_test.pk")

with_feature.sync_folder(new_folder)
with_feature.upload()
with_feature.finalize()
