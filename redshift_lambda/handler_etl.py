import psycopg2
import sys
import os
import csv
import boto3
import logging

#In the below code, "Order" refers to all info on one line (date, name, drinks purchased, total price etc).
#Whereas "Purchases" refer to just the drink information on the line. "Purchases" are one of several items in each "Order".

def start(event, context):
    print("Team One Pipeline")

    logging.getLogger().setLevel(0)

    extracted_dict = extract()
    transformed_dict = transform(extracted_dict)

    return transformed_dict
    

def extract():
    BUCKET_NAME = "cafe-data-data-pump-dev-team-1"
    FILE_NAME = "aberdeen_11-10-2020_19-49-26.csv"
    
    raw_data = read_from_s3(BUCKET_NAME, FILE_NAME) #TODO: will need updating to find file names from today
    raw_lines = convert_data_to_lines(raw_data)
    comma_separated_lines = split_lines(raw_lines)
    clean_split_orders = remove_whitespace_and_quotes(comma_separated_lines)
    combined_purchase_orders = combine_purchases(clean_split_orders)
    
    dict_ = generate_dictionary(combined_purchase_orders)
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

    debug_prints(transformed_dict)

    return transformed_dict


#EXTRACT
def read_from_s3(bucket, key):
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket = bucket, Key = key)

    data = s3_object['Body'].read().decode('utf-8')
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

    print("First Names of first 10 orders:")
    print(dict_["fname"][:10])

    print("Last Names of first 10 orders:")
    print(dict_["lname"][:10])

    print("Total Prices of first 10 orders:")
    print(dict_["total_price"][:10])

    print("Payment Methods of first 10 orders:")
    print(dict_["payment_method"][:10])

    print("Card Numbers of first 10 orders:")
    print(dict_["card_number"][:10])

    print("FIRST 10 PURCHASE INFOS")
    for purchase in dict_["purchase"][:10]:
        print("INFO:")

        print("Drink Sizes:")
        print(purchase["drink_size"])

        print("Drink Names:")
        print(purchase["drink_type"])

        print("Drink Flavours:")
        print(purchase["drink_flavour"])

        print("Drink Price:")
        print(purchase["drink_price"])

    print()
    print("To check invalid card numbers are correctly set to None, the following two numbers should be equal:")
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
        stripped_name = name.strip()
        index_of_first_space = stripped_name.find(" ")
        #finding only first space in case surname contains spaces (eg. Van Halen)

        first_name = stripped_name[:index_of_first_space]
        last_name = stripped_name[(index_of_first_space + 1):]

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
        
        if len(info_copy) == 3: #if there are three elements - drink name, flavour, price - then a flavour is present 
            return_list.append(info_copy)
        else:
            info_copy.insert(1, None) #insert None in place of a flavour if no flavour is present
            return_list.append(info_copy)

    return return_list  #return_list: [['Large Flavoured latte', 'Gingerbread', '2.85'], ['Large Flavoured latte', None, '2.85'], ['Large Flavoured latte', 'Gingerbread', '2.85']]


def check_for_drink_size(split_copy_list):
    return_list = []

    for split_info in split_copy_list:
        split_copy = split_info.copy() #['Large Flavoured latte', 'Gingerbread', '2.85']
        index_of_first_space = split_copy[0].find(" ")
        stripped_string = split_copy[0].strip()
        string_first_word_removed = stripped_string[(index_of_first_space + 1):]    

        if "Large " in split_copy[0].title():
            split_copy.insert(0, "Large")
            split_copy[1] = string_first_word_removed
            return_list.append(split_copy)
        elif "Regular " in split_copy[0].title():
            split_copy.insert(0, "Regular")
            split_copy[1] = string_first_word_removed
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
        drink_size.append(split_info[0])
        drink_type_list.append(split_info[1])
        drink_flavour_list.append(split_info[2])
        drink_price_list.append(split_info[3])

    return (drink_size, drink_type_list, drink_flavour_list, drink_price_list)


def remove_flavoured_words(drink_types):
    return_drink_types = []

    for drink in drink_types:
        new_drink = drink.title().replace("Flavoured ", "")
        return_drink_types.append(new_drink)

    return return_drink_types


def convert_string_list_to_int(string_list):
    return_list = []
    
    for string in string_list:
        new_string = string.replace(".", "")
        return_list.append(int(new_string))

    return return_list


def transform_purchases(purchases):
    list_of_dicts = []
    
    for purchase in purchases:
        new_dict = {}
        
        stripped_purchase = purchase.strip()
        drink_info_list = stripped_purchase.split(", ")

        split_info_list = make_drink_info_list(drink_info_list)
        split_info_list_with_nones = check_for_flavour(split_info_list) #this adds None to flavour value when no flavour is provided
        split_info_with_size = check_for_drink_size(split_info_list_with_nones) #this adds None to size value when size isn't provided, or splits size from drink name when it is
        drink_info_lists = make_split_info_list(split_info_with_size)

        drink_size_list = drink_info_lists[0]
        drink_type_list = drink_info_lists[1]
        drink_flavour_list = drink_info_lists[2]
        drink_price_list = drink_info_lists[3]

        drink_type_without_flavoured_words_list = remove_flavoured_words(drink_type_list)
        drink_price_as_float_list = convert_string_list_to_int(drink_price_list)

        new_dict["drink_size"] = drink_size_list
        new_dict["drink_type"] = drink_type_without_flavoured_words_list
        new_dict["drink_flavour"] = drink_flavour_list
        new_dict["drink_price"] = drink_price_as_float_list

        list_of_dicts.append(new_dict)
 
    return list_of_dicts


def clean_total_prices(raw_list):
    cleaned_total_prices = [(price*100) for price in raw_list]
    return cleaned_total_prices


def card_num_format(card_num_list):
    starred_numbers = []

    for num in card_num_list:
        if num.isnumeric() and 16 >= len(num) >= 12:
            formatted_number = num[-4:].rjust(len(num), "*") #replaces everything before the last 4 characters with stars
            starred_numbers.append(formatted_number)
        else:
            starred_numbers.append(None) #adds None as card number value if valid card number is not present

    return starred_numbers