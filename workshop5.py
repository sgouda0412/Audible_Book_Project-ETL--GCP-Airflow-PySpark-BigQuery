from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests

MYSQL_CONNECTION = "mysql_default"   # connection name that was set on Airflow to databases
CONVERSION_RATE_URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate" 

# Path to use
mysql_output_path = "/home/airflow/gcs/data/audible_data_merged.csv"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.csv"
final_output_path = "/home/airflow/gcs/data/output.csv"


# Function_1 :  to get the data from mysql database 
def get_data_from_mysql(transaction_path):
    # To get "transaction_path"

    # Use "MysqlHook" linked to MySQL from Connection that was set on Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)

    # Using "Hook" to query databases and saved to Pandas DataFrame
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    # Merge data 2 DataFrames above 
    df = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="BOOK_ID")

    # Save to CSV file at "transaction_path" ("/home/airflow/gcs/data/audible_data_merged.csv") 
    # Go to "Google Cloud Storage" automatically 
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")


# Function_2 : to get data from api 
def get_conversion_rate(conversion_rate_path):
    r = requests.get(CONVERSION_RATE_URL) 
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)


    # Index reset ("date index" set to "date column") 
    df.reset_index().rename(columns={"index": "date"})
    df.to_csv(conversion_rate_path, index=False)
    print(f"Output to {conversion_rate_path}")


# Function_3 : to Merge 2 DataFrame above, and saved to Output file
def merge_data(transaction_path, conversion_rate_path, output_path):

    # Read file from path 
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path) 

    # Convert timestamp column to date, and extract only date
    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date 
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date 

    # To merge 2 dataFrame
    final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")

    # Convert "Price" column to THAI baht, remove $ symbol, convert type to Float 
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
    final_df["Price"] = final_df["Price"].astype(float)

    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"] 
    final_df = final_df.drop(["date", "book_id"], axis=1)

    # save file to CSV 
    final_df.to_csv(output_path, index=False)
    print(f"Output to {output_path}")
    print("End of Workshop 4 !!! ")


# Create DAG 
with DAG(
    "exercise4_final_dag", 
    start_date=days_ago(1), 
    schedule_interval="@once",
    tags=["Workshop"]
) as dag: 

# To create task 
    t1 = PythonOperator(
        task_id = "get_data_from_mysql", 
        python_callable = get_data_from_mysql,
        op_kwargs={"transaction_path": mysql_output_path}
    )

    t2 = PythonOperator(
        task_id = "get_conversion_rate", 
        python_callable = get_conversion_rate, 
        op_kwargs={"conversion_rate_path": conversion_rate_output_path}
    )

    t3 = PythonOperator(
        task_id = "merge_data",
        python_callable = merge_data, 
        op_kwargs= {
            "transaction_path": mysql_output_path, 
            "conversion_rate_path": conversion_rate_output_path, 
            "output_path": final_output_path
        }
    )

# to create task 4 - load data to BigQuery with BashOperator (bq load)
    '''
    t4 = BashOperator(
        task_id = "load_to_bq", 
        bash_command= "bq load \
            --source_format=CSV --autodetect \
            workshop5.audible_data \
            gs://asia-east2-chanyanart/data/result.csv")
   '''
   

# to create task 4 - load data to BigQuery with GCSToBigQueryOperator
    t4 = GCSToBigQueryOperator(
        task_id="gcs_to_bq", 
        bucket="asia-east2-workshop5-03d65f08-bucket", 
        source_objects=["data/output.csv"], 
        destination_project_dataset_table="workshop5.audible_data2",
        skip_leading_rows=1,
        schema_fields=[
            {
                "mode": "NULLABLE",
                "name": "timestamp", 
                "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE", 
                "name": "user_id", 
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "country",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_ID",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_Title",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_Subtitle",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_Author",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_Narrator",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Audio_Runtime",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Audiobook_Type",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Categories",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Rating",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Total_No__of_Ratings",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Price",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "conversion_rate",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "THBPrice",
                "type": "FLOAT"
            }
        ],
        write_disposition="WRITE_TRUNCATE",
    )

# To create task dependencies 

    [t1, t2] >> t3 >> t4