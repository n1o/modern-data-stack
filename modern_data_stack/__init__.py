from dagster import Definitions, define_asset_job, make_values_resource, file_relative_path
from modern_data_stack.arxiv import kaggle_arxiv_data, arxiv_data

from modern_data_stack.resources.kaggle_resource import kaggle_fetcher
from modern_data_stack.resources.remote_fs_resource import s3_remote_file_system, local_remote_file_system

from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

ARXIV_DBT_PROJECT_PATH = file_relative_path(__file__, "../arxiv_dbt")
ARXIV_DBT_PROFILES = file_relative_path(__file__, "../arxiv_dbt/config")

dbt_assets = load_assets_from_dbt_project(
    project_dir=ARXIV_DBT_PROJECT_PATH, profiles_dir=ARXIV_DBT_PROFILES, key_prefix=["arxiv"]
)

BUCKET = "/home/mbarak/projects/modern-data-stack/tmp_s3"
ARXIV_PARQUET = "arxiv/arxiv_data.parquet"
PUBLICATIONS_PARQUET = "arxiv/publications.parquet"
CATEGORIES_PARQUET =  "arxiv/categories.parquet"

CONFIG = {
    "bucket": BUCKET,
    "arxiv_parquet": ARXIV_PARQUET ,
    "publications_parquet": PUBLICATIONS_PARQUET,
    "categories_parquet": CATEGORIES_PARQUET
}

default_config = {
    "ops": {"kaggle_arxiv_data": {"config": {"dataset_name": "cornell-university/arxiv"}}},
    'resources': {
        'kaggle_fetcher': {'config': { "tmp_path":  '/home/mbarak/Desktop/my-next-paper-tmp' }}, 
        "shared": { "config": CONFIG } 
    }
}

arxiv_job = define_asset_job("arxiv_job", selection=[kaggle_arxiv_data, arxiv_data] + dbt_assets, config=default_config)

defs = Definitions(
    assets=[kaggle_arxiv_data, arxiv_data] + dbt_assets,
    jobs=[arxiv_job],
    resources={
        "kaggle_fetcher": kaggle_fetcher,
        "remote_fs": local_remote_file_system,
        "shared": make_values_resource(
            bucket=str, 
            arxiv_parquet=str, 
            publications_parquet=str,
            categories_parquet=str
        ),
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": ARXIV_DBT_PROJECT_PATH,
                "profiles_dir": ARXIV_DBT_PROFILES,
                "vars": CONFIG
            }
        )
    }
)
