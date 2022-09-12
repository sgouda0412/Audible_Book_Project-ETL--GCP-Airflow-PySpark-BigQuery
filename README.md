# Audio_Book_project--Data-Engineering
This project is a part of "Road To Data Engineer 2.0" by Data TH. please check out the course here: (https://school.datath.com/)

# About the Project and Dataset 
We're working on Book Shop to help the owner identify which book got the highest sale. To get the answer, we need to ingest the transaction data and book name from 
mysql databases. And Join it together!!! We also need to get the API for converting US dollars to Thai baht. And to do the Cleaning Data with PySpark and JOIN them. 

We also use the Google Cloud Platform (GCP), the GCP services following:  
  - Google Cloud Storage (GCS) as our Data Lake 
  - Google Cloud Composer - to use "Apache Airflow" running Pipeline Orhestration 
  - Google Bigquery as our Data Warehouse 


## Workshop 1_Data Collection and Data Cleansing 
This task is about how to ingest data from 
   - Connecting database (mysql) - to get "audible_transaction" and "audible_data" csv. file
   - Get from Rest API - to get "Conversion_rate" 


## Workshop 2_Using pySpark 
   - To explore how to do Basic EDA (Exploratory Data Analysis) and use PySpark to data cleansing 
  (My CheatSheet note: https://gist.github.com/BestChanyanart/1f518a7dd857d017801acf6c9355eab5)
  
## Workshop 3_Working on Google Cloud Storage (GCS) 
   - To explore how to create Upload and Download function between local and GCS
    (Manual: https://cloud.google.com/storage/docs/uploading-objects) 


## Workshop 4_Working on Google Cloud Composer and Apache Airflow 
   - To create Pipeline to perform the process following Workshop 1 but running on Notebook. 
   - Set DAG ( Directed Acyclic Graph) 
   - Create Environment on Google Cloud Composer and Run "Apache Airflow" from the pipeline that we created. 


## Workshop 5_Load to Data Warehouse (BigQuery) 
   - Add the task from Workshop4, To load the data to BigQuery by using pipeline with Airflow 

