import psycopg2
import sys
import os
import csv

import boto3
from dotenv import load_dotenv
load_dotenv()

def start(event, context):
    print("hello world")

    print(os.getenv("DB_HOST"))
    print(int(os.getenv("DB_PORT")))
    print(os.getenv("DB_USER"))
    print(os.getenv("DB_PASS"))
    print(os.getenv("DB_NAME"))
    print(os.getenv("DB_CLUSTER"))

    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT"))
    user = os.getenv("DB_USER")
    passwd = os.getenv("DB_PASS")
    db = os.getenv("DB_NAME")
    cluster = os.getenv("DB_CLUSTER")

    try:
        client = boto3.client('redshift')
        creds = client.get_cluster_credentials(# Lambda needs these permissions as well DataAPI permissions
            DbUser=user,
            DbName=db,
            ClusterIdentifier=cluster,
            DurationSeconds=3600) # Length of time access is granted
    except Exception as ERROR:
        print("Credentials Issue: " + str(ERROR))
        sys.exit(1)

    print('got credentials')

    try:
        conn = psycopg2.connect(
            dbname=db,
            user=creds["DbUser"],
            password=creds["DbPassword"],
            port=port,
            host=host)
    except Exception as ERROR:
        print("Connection Issue: " + str(ERROR))
        sys.exit(1)

    print('connected')
    
    ### This is what Stuart had in the handler.py from the start.
    # try:
    #     cursor = conn.cursor()
    #     cursor.execute("create table test_table (id int)")
    #     cursor.close()
    #     conn.commit()
    #     conn.close()

    # except Exception as ERROR:
    #     print("Execution Issue: " + str(ERROR))
    #     sys.exit(1)

    # print('executed statement')


    data = read_from_s3("cafe-data-data-pump-dev-team-1", "aberdeen_11-10-2020_19-49-26.csv")
    print(data)
    dict_ = generate_dictionary(data)


### our ETL code is below this point.

def read_from_s3(bucket, key):
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=bucket,
                              Key=key)
    data = s3_object['Body'].read().decode('utf-8')

    return data


def convert_csv_string_to_list(string):
    string_without_brackets = string.replace("['", "").replace("']", "")

    return_list = string_without_brackets.split("', '")
    return return_list


def generate_dictionary(csv_strings):
    data = {"datetime": [], "location": [], "customer_name": [], "purchase": [], "total_price": [], "payment_method": [], "card_number": []}
    
    rows = []
    
    for string in csv_strings:
        new_list = convert_csv_string_to_list(string)
        rows.append(new_list)
    
    for row in rows:
        data["datetime"].append(row[0])
        data["location"].append(row[1])
        data["customer_name"].append(row[2])
        data["purchase"].append(row[3])
        data["total_price"].append(row[4])
        data["payment_method"].append(row[5])
        data["card_number"].append(row[6])

    return data