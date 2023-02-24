from dagster import Definitions, define_asset_job, make_values_resource
from modern_data_stack.arxiv import kaggle_arxiv_data, arxiv_data

from modern_data_stack.resources.kaggle_resource import kaggle_fetcher
from modern_data_stack.resources.remote_fs_resource import s3_remote_file_system, local_remote_file_system


default_config = {
    "ops": {"kaggle_arxiv_data": {"config": {"dataset_name": "cornell-university/arxiv"}}},
    'resources': {
        'kaggle_fetcher': {'config': { "tmp_path":  '/home/mbarak/Desktop/my-next-paper-tmp' }}, 
        "shared": { "config": 
            { "bucket": "/home/mbarak/projects/modern-data-stack/tmp_s3" }
        } 
    }
}

arxiv_job = define_asset_job("arxiv_job", selection=[kaggle_arxiv_data,arxiv_data], config=default_config)

defs = Definitions(
    assets=[kaggle_arxiv_data, arxiv_data],
    jobs=[arxiv_job],
    resources={
        "kaggle_fetcher": kaggle_fetcher,
        "remote_fs": local_remote_file_system,
        "shared": make_values_resource(bucket=str)
    }
)
