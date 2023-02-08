from kaggle.api.kaggle_api_extended import KaggleApi
import uuid
import os
from dagster import resource

class KaggleRessource:
    def __init__(sefl, tmp_path: str):
        sefl.tmp_path = tmp_path

    def fetch_dataset(self, dataset_name: str):
        api = KaggleApi()
        api.authenticate()

        name = str(uuid.uuid4())

        file_location = os.path.sep.join(self.tmp_path, name)
        
        api.dataset_download_files(dataset_name, file_location)

        return file_location

    
@resource(config_schema={"tmp_path": str})
def kaggle_fetcher(init_context):
    tmp_path = init_context.resource_config["tmp_path"]
    return KaggleRessource(tmp_path)