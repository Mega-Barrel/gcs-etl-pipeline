# 🛠️ GCS ETL Pipeline: Zomato API Data

This project showcases a simple and scalable ETL pipeline using Zomato restaurant data (sourced via Kaggle). It leverages Google Cloud Storage (GCS) to store both raw and processed data for downstream analytics and reporting.

## 📊 Project Highlights

- **Data Source**: [Zomato Restaurant Data on Kaggle](https://www.kaggle.com/datasets/shrutimehta/zomato-restaurants-data/data)
- **Storage**: Google Cloud Storage (GCS)
- **ETL Stack**: Python, Pandas, GCP SDK

## 🔁 ETL Workflow

1. **Extract**  
   Ingest the raw Kaggle dataset.

2. **Transform**  
   Clean and normalize columns such as:
   - City names
   - Cuisines
   - Ratings and votes

3. **Load**  
   - Save raw data to `gs://<your-bucket>/`
   - Save cleaned data to `gs://<your-bucket>/processed/`


## ⚙️ Setup Instructions

1. Clone the repository:

```bash
git clone https://github.com/Mega-Barrel/gcs-etl-pipeline.git
cd gcs-etl-pipeline
```

2. Install dependencies
```
pip install -r requirements.txt
```

3. Configure your GCS credentials_json file directory in src.common.ServiceAccountCredentials class

4. Run the pipeline
```bash
python3 run main.py
```
