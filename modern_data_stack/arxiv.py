from dagster import asset
import polars as pl

@asset(config_schema={"dataset_name": str}, required_resource_keys={ "kaggle_fetcher", "remote_fs", "shared"})
def kaggle_arxiv_data(context) -> str:
    dataset_name = context.op_config["dataset_name"]
    bucket = context.resources.shared["bucket"]
    
    path = context.resources.kaggle_fetcher.fetch_dataset(dataset_name)
    remote_path = f"{bucket}/arxiv/arxiv.json"


    context.resources.remote_fs.put_remote(path, remote_path)

    return remote_path

@asset(key_prefix="arxiv", required_resource_keys={"shared"})
def arxiv_data(context, kaggle_arxiv_data):

    bucket = context.resources.shared["bucket"]
    arxiv_parquet = context.resources.shared["arxiv_parquet"]

    context.log.info(f"Loading data from {kaggle_arxiv_data}")
    df = pl.scan_ndjson(kaggle_arxiv_data)
    df.sink_parquet

    mapped = df.with_columns([
        pl.col("authors_parsed").arr.eval(pl.element().arr.join(" ")).alias("authors"),
        pl.col("categories").str.split(by = " "),
        pl.col("update_date").str.strptime(pl.Date, "%F", strict=False)
    ]).select(pl.col(["id", "submitter", "authors", "title", "abstract", "categories", "update_date", "journal-ref"]))

    path = f"{bucket}/{arxiv_parquet}"

    context.log.info(f"Dumping parquet files into {path}")
    mapped.collect(streaming=True).write_parquet(path, row_group_size=10_000)
    return path