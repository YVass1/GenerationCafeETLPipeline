import psycopg2
import sys
import os
import csv
import boto3

def start(event, context):
    print("hello world")

    data = read_from_s3("cafe-data-data-pump-dev-team-1", "aberdeen_11-10-2020_19-49-26.csv")
    lines = convert_data_to_lines(data)
    split_orders = split_lines(lines)

    for order in split_orders:
        print(order)
    #print(data)
    #dict_ = generate_dictionary(data)
    #debug_prints(dict_)


### our ETL code is below this point.

def read_from_s3(bucket, key):
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=bucket,
                              Key=key)
    data = s3_object['Body'].read().decode('utf-8')

    return data


def convert_data_to_lines(data):
    data_without_brackets = data.replace("['", "").replace("']", "")
    return data_without_brackets.split("\n")
    

def split_lines(lines):
    return return_list = []
    
    for line in lines:
        if len(line) > 10:
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


def debug_prints(dict_):
    print(dict_["datetime"])
    print(dict_["location"])
    print(dict_["customer_name"])
    print(dict_["purchase"])
    print(dict_["total_price"])
    print(dict_["payment_method"])
    print(dict_["card_number"])

    print("total locations: " + str(len(dict_["location"])))
    print("total card nunmbers: " + str(len(dict_["card_number"])))