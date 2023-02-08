from dagster import op, job
from kaggle.api.kaggle_api_extended import KaggleApi


@op()
def kaggle_arxiv_data(context, dataset_name) -> str:
    path = context.resources.kaggle_fetcher.fetch_data(dataset_name)
    