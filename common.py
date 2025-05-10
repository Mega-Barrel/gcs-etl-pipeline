""" Google Cloud Helper function """

from google.oauth2 import service_account
from google.cloud import storage

class ServiceAccountCredentials:
    """Create credentials from service account JSON file."""

    def __init__(self, credentials_path='credentials.json'):
        self.__credentials_path = credentials_path
        self.__credentials = self.__create_connection()

    def __create_connection(self):
        """ Make connection using service account"""
        return service_account.Credentials.from_service_account_file(self.__credentials_path)

    def get_credentials(self):
        """ Return the connection object """
        return self.__credentials

class BucketConfig:
    """ Bucket config"""
    BUCKET_NAME = 'sj-json-bucket'
    GCS_PREFIX = 'json_data/'
    LOCAL_JSON_FILE = 'raw-data/'

class CloudStorage:
    """ CloudStorage helper class to perform creation and deletion of buckets """
    def __init__(self, credentials):
        # Initialize GCS client
        self.storage_client = storage.Client(credentials=credentials)

    def create_bucket(self, bucket_name):
        """ Create a GCS Bucket if it doesn't exists """
        bucket = self.storage_client.bucket(
            bucket_name=bucket_name
        )
        if not bucket.exists():
            bucket = self.storage_client.create_bucket(
                bucket_or_name = bucket_name,
                location = "asia-south1"
            )
            print(f"Bucket {bucket_name} created.")
        else:
            print(f"Bucket {bucket_name} already exists.")
        return bucket

    def delete_bucket(self, bucket_name, force = False):
        """ Delete a GCS bucket. Set force=True to delete even if not empty """
        bucket = self.storage_client.bucket(bucket_name)
        if force:
            blobs = list(bucket.list_blobs())
            for blob in blobs:
                blob.delete()
                print(f"Deleted blob: {blob.name}")
        bucket.delete()
        print(f"Bucket {bucket_name} deleted.")

if __name__ == "__main__":
    # Path to your credentials JSON file
    CREDS = ServiceAccountCredentials().get_credentials()
    BUCKET = CloudStorage(
        credentials=CREDS
    )
    BUCKET.create_bucket(
        bucket_name=BucketConfig.BUCKET_NAME
    )
