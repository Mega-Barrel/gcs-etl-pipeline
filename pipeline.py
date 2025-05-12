
import json

from common import (
    CloudStorage,
    ServiceAccountCredentials,
    BucketConfig
)

class Extract:
    """ Read raw JSON files from Google Storage Bucket """
    def __init__(self, bucket_files):
        self.data = []
        self.bucket_files = bucket_files

    def execute(self):
        """ Method to read data from CloudStorage Bucket"""
        for blob in self.bucket_files:
            if blob.name in ['file5.json']:
                content = blob.download_as_text()
                self.data.extend(json.loads(content))
        print(f"[Extract] Fetched {len(self.data)} JSON Objects.")
        return self.data

class Transform:
    """ Clean and apply Transformation on JSON files """
    def __init__(self, raw_data):
        self.transformed_data = []
        self.raw_data = raw_data

    def execute(self):
        """ Method to transforma and clean raw data"""
        for entry in self.raw_data:
            restaurants = entry.get("restaurants")
            if restaurants:
                for restaurant in restaurants:
                    json_dump = restaurant.get("restaurant", {})
                    cleaned = {
                        'restaurant_name': json_dump.get('name'),
                        'menu_url': json_dump.get('menu_url'),
                        'photos_url': json_dump.get('photos_url'),
                        'events_url': json_dump.get('events_url'),
                        'is_online_delivery': bool(json_dump.get('has_online_delivery', 0)),
                        'average_cost_for_two': json_dump.get('average_cost_for_two'),
                        'total_votes': json_dump.get('votes'),
                        'user_rating': json_dump.get('user_rating', {}).get('aggregate_rating'),
                        'latitude': json_dump.get('location', {}).get('latitude'),
                        'longitude': json_dump.get('location', {}).get('longitude'),
                        'location': json_dump.get('location', {}).get('address'),
                        'city': json_dump.get('location', {}).get('city'),
                        'locality': json_dump.get('location', {}).get('locality'),
                        'cuisines': json_dump.get('cuisines'),
                        'featured_image': json_dump.get('featured_image'),
                        'offers': json_dump.get('offers'),
                        'currency': json_dump.get('currency'),
                    }
                    self.transformed_data.append(cleaned)
        print(f"[Transform] Transformed {len(self.transformed_data)} records.")
        return self.transformed_data

class Load:
    """ Dump clean data back to Google Storage Bucket """
    def __init__(self, storage_client, bucket_name, data):
        self.storage_client = storage_client
        self.bucket_name = bucket_name
        self.data = data

    def execute(self, destination_blob_name="cleaned/cleaned_data.json"):
        """ Loads the final data to Cloud Storage Bucket"""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(
            data=json.dumps(self.data, indent=4),
            content_type='application/json'
        )
        print(f"[Load] Uploaded cleaned data to gs://{self.bucket_name}/{destination_blob_name}")

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
        Load(storage_client=self.storage_client, bucket_name=self.bucket_name, data=transformer).execute()

if __name__ == "__main__":
    # Path to your credentials JSON file
    # CREDS = ServiceAccountCredentials().get_credentials()
    # BUCKET = CloudStorage(
    #     credentials=CREDS
    # )
    # BUCKET_FILES = BUCKET.read_bucket(
    #     bucket_name=BucketConfig.BUCKET_NAME
    # )
    # pipeline = ELTPipeline(BUCKET_FILES, CREDS)
    # pipeline.run()

    # Step 1: Get Credentials
    CREDS = ServiceAccountCredentials().get_credentials()

    # Step 2: Create GCS client
    cloud_storage = CloudStorage(credentials=CREDS)
    storage_client = cloud_storage.storage_client

    # Step 3: Read files from bucket
    BUCKET_FILES = cloud_storage.read_bucket(bucket_name=BucketConfig.BUCKET_NAME)

    # Step 4: Run pipeline
    pipeline = ELTPipeline(
        bucket_name=BucketConfig.BUCKET_NAME,
        creds=storage_client,
        bucket_files=BUCKET_FILES
    )
    pipeline.run()
