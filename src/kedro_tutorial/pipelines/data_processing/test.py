import pandas as pd
import clearml

task = clearml.Task.init(project_name="func_test", task_name="function_test")


def _is_true(x):
    return x == "t"


def _parse_percentage(x):
    x = x.str.replace("%", "")
    x = x.astype(float) / 100
    return x


def preprocess_companies(companies: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for companies.

    Args:
        companies: Raw data.
    Returns:
        Preprocessed data, with `company_rating` converted to a float and
        `iata_approved` converted to boolean.
    """
    companies["iata_approved"] = _is_true(companies["iata_approved"])
    companies["company_rating"] = _parse_percentage(companies["company_rating"])
    return companies


if __name__ == "__main__":
    task.create_function_task(
        func=preprocess_companies,
        func_name="preprocess_companies",
        companies=pd.read_csv("https://quantumblacklabs.github.io/kedro/companies.csv"),
    )
