from clearml import Task, Dataset
from pathlib import Path
import pandas as pd

from tempfile import mkdtemp


# Connecting ClearML with the current process,
# from here on everything is logged automatically
task = Task.init(
    project_name="kedro_tutorial_clearml/de", task_name="create_model_input_table"
)

companies_task = Task.get_task(
    project_name="kedro_tutorial_clearml/de", task_name="preprocess_companies"
)
shuttle_task = Task.get_task(
    project_name="kedro_tutorial_clearml/de", task_name="preprocess_shuttles"
)
tdata = Dataset.get(dataset_project="kedro_tutorial_clearml/data", dataset_name="raw")
tdata_folder = tdata.get_local_copy()
data_path = list(Path(tdata_folder).glob("*.reviews.*"))[0]

reviews = pd.read_csv(data_path)

preprocess_shuttles = pd.read_csv(shuttle_task.artifacts["data"].get())
preprocess_companies = pd.read_csv(companies_task.artifacts["data"].get())

rated_shuttles = preprocess_shuttles.merge(reviews, left_on="id", right_on="shuttle_id")
model_input_table = rated_shuttles.merge(
    preprocess_companies, left_on="company_id", right_on="id"
)
model_input_table = model_input_table.dropna()

# add and upload local file containing our toy dataset
task.upload_artifact("model_input_table", artifact_object=model_input_table)

with_feature = Dataset.create(
    "name does not matter - the task is the feature",
    dataset_project="kedro_tutorial_clearml/de",
    parent_datasets=[companies_task.id, shuttle_task.id, tdata.id],
    use_current_task=True,
)  # This boolean is the main point actually!!!

new_folder = with_feature.get_mutable_local_copy(mkdtemp())
print(f"new_folder is:{new_folder}")
# overwrite with new train df (with the added)
model_input_table.to_csv(new_folder + "/model_input_table.csv", index=False)
with_feature.sync_folder(new_folder)
with_feature.upload()
with_feature.finalize()
