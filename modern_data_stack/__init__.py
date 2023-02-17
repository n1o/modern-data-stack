from dagster import Definitions
from modern_data_stack.arxiv import arxiv_job, kaggle_arxiv_data

defs = Definitions(
    assets=[],
    jobs=[arxiv_job]
)
