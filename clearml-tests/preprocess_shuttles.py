from clearml import Task, Dataset
import pandas as pd
import matplotlib.pyplot as plt
from tempfile import mkdtemp
from pathlib import Path

# create an dataset experiment
task = Task.init(
    project_name="kedro_tutorial_clearml/de", task_name="preprocess_shuttles"
)

# only create the task, we will actually execute it later
# task.execute_remotely()


def _is_true(x):
    return x == "t"


def _parse_money(x):
    x = x.str.replace("$", "").str.replace(",", "")
    x = x.astype(float)
    return x


tdata = Dataset.get(dataset_project="kedro_tutorial_clearml/data", dataset_name="raw")
tdata_folder = tdata.get_local_copy()
data_path = list(Path(tdata_folder).glob("*.shuttles.*"))[0]

shuttles = pd.read_excel(data_path)

shuttles["d_check_complete"] = _is_true(shuttles["d_check_complete"])
shuttles["moon_clearance_complete"] = _is_true(shuttles["moon_clearance_complete"])
shuttles["price"] = _parse_money(shuttles["price"])

# add and upload local file containing our toy dataset
task.upload_artifact("shuttles", artifact_object=shuttles)

with_feature = Dataset.create(
    "name does not matter - the task is the feature",
    dataset_project="kedro_tutorial_clearml/de",
    parent_datasets=[tdata.id],
    use_current_task=True,
)  # This boolean is the main point actually!!!

new_folder = with_feature.get_mutable_local_copy(mkdtemp())
print(f"new_folder is:{new_folder}")
# overwrite with new train df (with the added)
shuttles.to_csv(new_folder + "/shuttles.csv", index=False)
with_feature.sync_folder(new_folder)
with_feature.upload()
with_feature.finalize()
