from dagster import asset, job
from modern_data_stack.kaggle_resource import kaggle_fetcher
from dagster_aws.s3 import s3_resource

@asset(config_schema={"dataset_name": str}, required_resource_keys={ "kaggle_fetcher", "s3"})
def kaggle_arxiv_data(context) -> str:
    dataset_name = context.op_config["dataset_name"]
    path = context.resources.kaggle_fetcher.fetch_dataset(dataset_name)
    return path

default_config = {
    "ops": {"kaggle_arxiv_data": {"config": {"dataset_name": "cornell-university/arxiv'"}}},
    'resources': {'kaggle_fetcher': {'config': {'tmp_path': '/home/mbarak/Desktop/my-next-paper-tmp'}}}
    }

@job(config=default_config, resource_defs={"kaggle_fetcher": kaggle_fetcher, "s3": s3_resource})
def arxiv_job():
    path_to_dataset = kaggle_arxiv_data()