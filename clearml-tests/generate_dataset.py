from clearml import Dataset, StorageManager

reviews = StorageManager.get_local_copy(
    "https://quantumblacklabs.github.io/kedro/reviews.csv"
)
companies = StorageManager.get_local_copy(
    "https://quantumblacklabs.github.io/kedro/companies.csv"
)
shuttle = StorageManager.get_local_copy(
    "https://quantumblacklabs.github.io/kedro/shuttles.xlsx"
)

dataset = Dataset.create(
    dataset_name="raw", dataset_project="kedro_tutorial_clearml/data"
)

dataset.add_files(companies)
dataset.add_files(shuttle)
dataset.add_files(reviews)
dataset.upload()
dataset.finalize()
