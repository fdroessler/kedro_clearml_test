from clearml import Task, Dataset
import pandas as pd
import matplotlib.pyplot as plt
from tempfile import mkdtemp
from pathlib import Path

# create an dataset experiment
task = Task.init(
    project_name="kedro_tutorial_clearml/de", task_name="preprocess_companies"
)

# only create the task, we will actually execute it later
# task.execute_remotely()

logger = task.get_logger()


def _is_true(x):
    return x == "t"


def _parse_percentage(x):
    x = x.str.replace("%", "")
    x = x.astype(float) / 100
    return x


tdata = Dataset.get(dataset_project="kedro_tutorial_clearml/data", dataset_name="raw")
tdata_folder = tdata.get_local_copy()
data_path = list(Path(tdata_folder).glob("*.companies.csv"))[0]

companies = pd.read_csv(data_path)

companies["iata_approved"] = _is_true(companies["iata_approved"])
companies["company_rating"] = _parse_percentage(companies["company_rating"])
fig, axes = plt.subplots()
companies["total_fleet_count"].hist(bins=50, ax=axes)

logger.report_matplotlib_figure("hist", "hist1", fig)

# add and upload local file containing our toy dataset
task.upload_artifact("companies", artifact_object=companies)

with_feature = Dataset.create(
    "name does not matter - the task is the feature",
    dataset_project="kedro_tutorial_clearml/de",
    parent_datasets=[tdata.id],
    use_current_task=True,
)  # This boolean is the main point actually!!!

new_folder = with_feature.get_mutable_local_copy(mkdtemp())
print(f"new_folder is:{new_folder}")
# overwrite with new train df (with the added)
companies.to_csv(new_folder + "/companies.csv", index=False)
with_feature.sync_folder(new_folder)
with_feature.upload()
with_feature.finalize()
