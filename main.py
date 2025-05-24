""" Main.py file to execute Extract, Transform, Load Job"""

from src.common import (
    CloudStorage,
    ServiceAccountCredentials,
    BucketConfig
)

from src.pipeline import (
    Extract,
    Transform,
    Load
)

class ELTPipeline:
    """ Run Extract → Transform → Load """
    def __init__(self, bucket_name, creds, bucket_files):
        self.bucket_name = bucket_name
        self.storage_client = creds
        self.bucket_files = bucket_files

    def run(self):
        """ Method to execute ETL Pipeline"""
        extractor = Extract(bucket_files=self.bucket_files).execute()

        transformer = Transform(raw_data=extractor).execute()

        Load(
            storage_client=self.storage_client,
            bucket_name=self.bucket_name,
            data=transformer
        ).execute()

if __name__ == "__main__":
    CREDS = ServiceAccountCredentials().get_credentials()

    cloud_storage = CloudStorage(credentials=CREDS)

    # Uncomment the below code to upload local dataset to Google Cloud Storage
    # cloud_storage.insert_blob(bucket_name=BucketConfig.BUCKET_NAME, local_dir='raw-data/')

    storage_client = cloud_storage.storage_client

    BUCKET_FILES = cloud_storage.read_bucket(bucket_name=BucketConfig.BUCKET_NAME)

    pipeline = ELTPipeline(
        bucket_name=BucketConfig.BUCKET_NAME,
        creds=storage_client,
        bucket_files=BUCKET_FILES
    )
    pipeline.run()
