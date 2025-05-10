
import os
import json
from google.cloud import storage


class Extract:
    """ Read raw JSON files from Google Storage Bucket """
    def __init__(self):
        pass

    def execute(self):
        pass

class Transform:
    """ Clean and apply Transformation on JSON files """
    def __init__(self):
        pass

    def execute(self):
        pass

class Load:
    """ Dump clean data back to Google Storage Bucket """
    def __init__(self):
        pass

    def execute(self):
        pass

class ELTPipeline:
    """ Orchestrates the EL (Extract and Load) process """
    def __init__(
        self,
        num_records: int,
        start_date: datetime,
        project_id: str,
        dataset_id: str,
        table_id: str
    ):
        pass

