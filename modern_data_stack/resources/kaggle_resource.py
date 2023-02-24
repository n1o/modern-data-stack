from kaggle.api.kaggle_api_extended import KaggleApi
from dagster import resource
import os
from uuid import uuid4

class KaggleRessource:
    def __init__(self, log, tmp_path: str):
        self.log = log
        self.tmp_path = tmp_path

    def fetch_dataset(self, dataset_name: str):
        api = KaggleApi()
        api.authenticate()

        file_location = os.path.sep.join([self.tmp_path, "kaggle-data"])
        
        if not os.path.exists(file_location):
            os.mkdir(file_location)

        self.log.info(f"Downloading dataset {dataset_name} to path {file_location}")
        
        api.dataset_download_files(dataset_name, file_location, quiet=False, unzip=True, force=False)

        [file] = os.listdir(file_location)

        return os.path.sep.join([file_location, file])

    
@resource(config_schema={"tmp_path": str})
def kaggle_fetcher(init_context):
    tmp_path = init_context.resource_config["tmp_path"]
    return KaggleRessource(init_context.log, tmp_path)