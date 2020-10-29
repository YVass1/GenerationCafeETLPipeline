import psycopg2
import sys
import os
import csv
import boto3

def start(event, context):
    print("hello world")

    #EXTRACT
    data = read_from_s3("cafe-data-data-pump-dev-team-1", "aberdeen_11-10-2020_19-49-26.csv")
    lines = convert_data_to_lines(data)
    split_orders = split_lines(lines)
    clean_split_orders = remove_whitespace_and_quotes(split_orders)
    combined_purchase_orders = combine_purchases(clean_split_orders)
    
    dict_ = generate_dictionary(combined_purchase_orders)
    debug_prints(dict_)

#EXTRACT
def read_from_s3(bucket, key):
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=bucket,
                              Key=key)
    data = s3_object['Body'].read().decode('utf-8')

    return data


def convert_data_to_lines(data):
    return_list = data.split("\n")
    return return_list
    

def split_lines(lines):
    return_list = []
    
    for line in lines:
        if len(line) > 10:
            return_list.append(line.split(","))
    
    return return_list


def remove_whitespace_and_quotes(lists):
    return_lists = []
    
    for list_ in lists:
        new_list = []
        
        for item in list_:
            new_list.append(item.replace('"', '').strip())
            
        return_lists.append(new_list)
    
    return return_lists


def combine_purchases(orders):
    return_lists = []
    
    for order in orders:
        NON_PURCHASE_ITEM_COUNT = 6
        order_item_count = len(order)
        
        purchase_items = order[3:-3]
        combined_purchase = combine_items_into_string(purchase_items)
        
        new_list = order[:3]
        new_list.append(combined_purchase)
        new_list.append(order[-3])
        new_list.append(order[-2])
        new_list.append(order[-1])
        
        return_lists.append(new_list)
    
    return return_lists


def combine_items_into_string(items):
    separator = ', '
    return separator.join(items)


def generate_dictionary(rows):
    data = {"datetime": [], "location": [], "customer_name": [], "purchase": [], "total_price": [], "payment_method": [], "card_number": []}
    
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