import psycopg2
import sys
import os
import csv
import boto3

def start(event, context):
    print("hello world")

    extracted_dict = extract()
    transformed_dict = transform(extracted_dict)
    

def extract():
    data = read_from_s3("cafe-data-data-pump-dev-team-1", "aberdeen_11-10-2020_19-49-26.csv")
    lines = convert_data_to_lines(data)
    split_orders = split_lines(lines)
    clean_split_orders = remove_whitespace_and_quotes(split_orders)
    combined_purchase_orders = combine_purchases(clean_split_orders)
    
    dict_ = generate_dictionary(combined_purchase_orders)
    #debug_prints(dict_)
    return dict_


def transform(dict_):
    transformed_dict = {}

    transformed_dict["datetime"] = clean_datetimes(dict_["datetime"])
    transformed_dict["location"] = dict_["location"]
    transformed_dict["fname"], transformed_dict["lname"] = clean_customer_names(dict_["customer_name"])
    transformed_dict["purchase"] = transform_purchases(dict_["purchase"])
    transformed_dict["total_price"] = clean_total_prices(dict_["total_price"])
    transformed_dict["payment_method"] = dict_["payment_method"]
    transformed_dict["card_number"] = card_num_format(dict_["card_number"])

    return transformed_dict


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

#TRANSFORM
def clean_datetimes(raw_list):
    cleaned_datetimes = []

    for datetime in raw_list:
        cleaned_datetimes.append(datetime[6:10] + "-" + datetime[3:5] + "-" + datetime[0:2] + " " + datetime[11:16] + ":00")

    return cleaned_datetimes


def clean_customer_names(customer_names):
    fname_list = []
    lname_list = []

    for name in customer_names:
        strip_whitespace = name.strip()
        index_for_first_space = strip_whitespace.find(" ")
        name_length = len(strip_whitespace)

        first_name = strip_whitespace[0:index_for_first_space]
        last_name = strip_whitespace[(index_for_first_space + 1):name_length]

        fname_list.append(first_name.title())
        lname_list.append(last_name.title())

    return (fname_list, lname_list)


def make_drink_info_list(drink_info_list):
    split_info_list = []

    for drink_info in drink_info_list:
        split_info_list.append(drink_info.split(" - ")) #drink_info_list: ["Large Flavoured latte - Gingerbread - 2.85", "Large Flavoured latte - Vanilla - 2.85", "Large Flavoured latte - Gingerbread - 2.85"]

    return split_info_list #split_info_list : [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', 'Vanilla', '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]


def check_for_flavour(split_info_list):
    return_list = []
    
    for split_info in split_info_list:
        info_copy = split_info.copy()
        
        if len(info_copy) == 3:
            return_list.append(info_copy)
        else:
            info_copy.insert(1, None)
            return_list.append(info_copy)

    return return_list  #return_list: [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', None, '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]


def check_for_drink_size(split_copy_list):
    return_list = []
    for split_info in split_copy_list:
        print(split_info)
        split_copy = split_info.copy() #['Large Flavoured latte', 'Gingerbread', '2.85']
        looking_for_space = split_copy[0].find(" ")
        stripped_string = split_copy[0].strip()
        remove_drink_size = stripped_string[(looking_for_space+1):]     
        if "Large " in split_copy[0].title():
            split_copy.insert(0, "Large")
            split_copy.insert(1, remove_drink_size)
            del split_copy[2]
            return_list.append(split_copy)
        elif "Regular " in split_copy[0].title():
            split_copy.insert(0, "Regular")
            split_copy.insert(1, remove_drink_size)
            del split_copy[2]
            return_list.append(split_copy)
        else:
            split_copy.insert(0, None)
            return_list.append(split_copy)
    return return_list 


def make_split_info_list(split_info_list): #split_info_list:[["Large Flavoured latte", "Gingerbread", "2.85"], ["Large Flavoured latte", "Vanilla", "2.85"], ["Large Flavoured latte", "Gingerbread", "2.85"]]
    drink_size = []
    drink_type_list = []
    drink_flavour_list = []
    drink_price_list = []
    
    for split_info in split_info_list:
        drink_size.append (split_info[0])
        drink_type_list.append(split_info[1])
        drink_flavour_list.append(split_info[2])
        drink_price_list.append(split_info[3])

    return (drink_size, drink_type_list, drink_flavour_list, drink_price_list)


def transform_purchases(purchases):
    
    list_of_dicts = []
    
    for purchase in purchases:
        new_dict = {}
        
        stripped_purchase = purchase.strip()
        drink_info_list = stripped_purchase.split(", ") #

        split_info_list = make_drink_info_list(drink_info_list)
        split_info_list_with_nones = check_for_flavour(split_info_list) #this adds None to flavour index when no flavour is provided
        split_info_with_size = check_for_drink_size(split_info_list_with_nones)
        drink_info_lists = make_split_info_list(split_info_with_size)

        drink_size_list = drink_info_lists[0]
        drink_type_list = drink_info_lists[1]
        drink_flavour_list = drink_info_lists[2]
        drink_price_list = drink_info_lists[3]

        new_dict["drink_size"] = drink_size_list
        new_dict["drink_type"] = drink_type_list
        new_dict["drink_flavour"] = drink_flavour_list
        new_dict["drink_price"] = drink_price_list

        list_of_dicts.append(new_dict)
        # print("drink_info_list")
        # print(drink_info_list)
        # print("split_info_list")
        # print(split_info_list)
        # print("split_info_list_with_nones")
        # print(split_info_list_with_nones)
        # print("split_info_with_size")
        # print(split_info_with_size)   
    return list_of_dicts


def clean_total_prices(raw_list):
    cleaned_total_prices = [float(price) for price in raw_list]
    return cleaned_total_prices


def card_num_format(card_num_list):
    num_star = []

    for num in card_num_list:
        if not num or num.isspace():
            num_star.append(None)
        elif num.isnumeric() and 16 >= len(num) >= 12:
            format_card = num[-4:].rjust(len(num), "*")
            num_star.append(format_card)

    return num_star
