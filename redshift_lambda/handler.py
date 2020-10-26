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
    try:
        cursor = conn.cursor()
        cursor.execute("create table test_table (id int)")
        cursor.close()
        conn.commit()
        conn.close()

    except Exception as ERROR:
        print("Execution Issue: " + str(ERROR))
        sys.exit(1)

    print('executed statement')



    rows = get_raw_csv_data()
    data = generate_dictionary(rows)

    test = transform_purchases(["Large Flavoured latte - Gingerbread - 2.85, Coffee - Hazelnut - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85"])


### our ETL code is below this point.

def get_raw_csv_data():
    filepath = "aberdeen_11-10-2020_19-49-26.csv"
    rows = []

    with open(filepath, 'r') as file_:
        reader = csv.reader(file_)
        
        for line in reader:
            rows.append(str(line))

    return rows


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


def make_drink_info_list(drink_info_list):
    split_info_list = []

    for drink_info in drink_info_list:
        split_info_list.append(drink_info.split(" - "))

    return split_info_list


def check_for_flavour(split_info_list):
    return_list = []
    
    for split_info in split_info_list:
        info_copy = split_info.copy()
        
        if len(info_copy) == 3:
            return_list.append(info_copy)
        else:
            return_list.append(info_copy.insert(1, None))
        
    return return_list


def make_split_info_list(split_info_list):
    drink_type_list = []
    drink_flavour_list = []
    drink_price_list = []
    
    for split_info in split_info_list:
        drink_type_list.append(split_info[0])
        drink_flavour_list.append(split_info[1])
        drink_price_list.append(split_info[2])

    return (drink_type_list, drink_flavour_list, drink_price_list)


def transform_purchases(purchases):
    
    list_of_dicts = []
    
    for purchase in purchases:
        new_dict = {}
        
        stripped_purchase = purchase.strip()
        drink_info_list = stripped_purchase.split(", ")

        split_info_list = make_drink_info_list(drink_info_list)
        split_info_list_with_nones = check_for_flavour(split_info_list) #this adds None to flavour index when no flavour is provided
        drink_info_lists = make_split_info_list(split_info_list_with_nones)
        

        drink_type_list = drink_info_lists[0]
        drink_flavour_list = drink_info_lists[1]
        drink_price_list = drink_info_lists[2]

        new_dict["drink_type"] = drink_type_list
        new_dict["drink_flavour"] = drink_flavour_list
        new_dict["drink_price"] = drink_price_list

        list_of_dicts.append(new_dict)
        
    return list_of_dicts
    