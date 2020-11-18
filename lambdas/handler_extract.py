import psycopg2
from dotenv import load_dotenv
import csv
import os
import boto3
import json
import logging


def start(event, context):
    print("Team One Pipeline")

    load_dotenv()
    RAW_DATA_BUCKET_NAME = os.getenv("RAW_DATA_BUCKET_NAME")
    PAYLOAD_BUCKET_NAME = os.getenv("PAYLOAD_BUCKET_NAME")
    ETOTQUEUE_URL = os.getenv("ETOTQUEUE_URL")
     
    logging.getLogger().setLevel(0)
    
    file_to_extract_list = get_keys_to_extract(event)

    extracted_dict_list = []

    for file_ in file_to_extract_list:
        if file_ == None:
            return None

        extracted_dict = extract(RAW_DATA_BUCKET_NAME, file_)
        json_dict = json_serialize_dict(extracted_dict)
        file_ref = send_json_to_s3(json_dict, PAYLOAD_BUCKET_NAME, file_)
        send_file_ref_to_queue(file_ref, ETOTQUEUE_URL)
        debug_prints(extracted_dict)
        extracted_dict_list.append(extracted_dict)

    return extracted_dict_list


def get_keys_to_extract(event):
    keys_list = []
    for record in event["Records"]:
        keys_list.append(record["s3"]["object"]["key"])

    return keys_list


def json_serialize_dict(dict_):
    json_dict = json.dumps(dict_)

    return json_dict


def send_json_to_s3(json_dict, bucket_name, filename):
    s3 = boto3.client('s3')

    new_file_key = filename.replace(".csv", "") + "_extracted.json"
    s3.Bucket(bucket_name).put_object(Key = new_file_key, Body = json_dict)

    return new_file_key


def send_file_ref_to_queue(file_ref, queue_url):
    sqs = boto3.client('sqs')

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl = queue_url,
        DelaySeconds = 1,
        MessageAttributes = {
            'TestAttribute': {
                'DataType': 'String',
                'StringValue': 'Hello World'
            },
        },
        MessageBody = file_ref
    )

    print(response['MessageId'])


def extract(bucket_name, cafe_csv_key_name):
    raw_data = read_from_s3(bucket_name, cafe_csv_key_name)
    raw_lines = convert_data_to_lines(raw_data)
    comma_separated_lines = split_lines(raw_lines)
    clean_split_orders = remove_whitespace_and_quotes(comma_separated_lines)
    combined_purchase_orders = combine_purchases(clean_split_orders)
    
    dict_ = generate_dictionary(combined_purchase_orders)
    return dict_


def read_from_s3(bucket, cafe_csv_key):
    s3 = boto3.client('s3')

    s3_raw_cafe_data = s3.get_object(Bucket = bucket, Key = cafe_csv_key)

    data = s3_raw_cafe_data['Body'].read().decode('utf-8')

    return data


def convert_data_to_lines(data):
    return_list = data.split("\n")
    return return_list
    

def split_lines(lines):
    return_list = []
    
    for line in lines:
        if len(line) > 10: #removing junk lines - a line with fewer than 10 characters does not contain valid info
            return_list.append(line.split(","))
    
    return return_list


def remove_whitespace_and_quotes(orders):
    split_orders = []
    
    for order in orders:
        new_list = []
        
        for item in order:
            new_list.append(item.replace('"', '').strip())
            
        split_orders.append(new_list)
    
    return split_orders


def combine_purchases(orders):
    return_orders = []
    
    for order in orders:
        items_before_purchase = order[:3]
        purchase_items = order[3:-3] 
        items_after_purchase = order[-3:]
        #the first 3 and last 3 items are consistent - the middle items are drinks from the purchase, and could vary in length

        joined_purchase_string = combine_items_into_string(purchase_items)

        new_list = items_before_purchase + [joined_purchase_string] + items_after_purchase
        return_orders.append(new_list)
    
    return return_orders


def combine_items_into_string(items):
    separator = ', '
    return separator.join(items)


def generate_dictionary(orders):
    data = {"datetime": [], "location": [], "customer_name": [], "purchase": [], "total_price": [], "payment_method": [], "card_number": []}
    
    for order in orders:
        data["datetime"].append(order[0])
        data["location"].append(order[1])
        data["customer_name"].append(order[2])
        data["purchase"].append(order[3])
        data["total_price"].append(order[4])
        data["payment_method"].append(order[5])
        data["card_number"].append(order[6])

    return data

def debug_prints(dict_):
    print("Dates of first 10 orders:")
    print(dict_["datetime"][:10])

    print("Locations of first 10 orders:")
    print(dict_["location"][:10])

    print("Names of first 10 orders:")
    print(dict_["customer_name"][:10])

    print("Total Prices of first 10 orders:")
    print(dict_["total_price"][:10])

    print("Payment Methods of first 10 orders:")
    print(dict_["payment_method"][:10])

    print("Card Numbers of first 10 orders:")
    print(dict_["card_number"][:10])

    print("Purchases of first 10 orders:")
    print(dict_["purchase"][:10])

    print()
    print("To check invalid card numbers are correctly set to None, the following two numbers should be equal:")
    print("total locations: " + str(len(dict_["location"])))
    print("total card nunmbers: " + str(len(dict_["card_number"])))
