import psycopg2
import logging
import json
import boto3
import uuid
from dotenv import load_dotenv
import os

def start(event, context):
    print("Team One Pipeline")

    load_dotenv()
    logging.getLogger().setLevel(0)

    TTOLQUEUE_URL = os.getenv("TTOLQUEUE_URL")

    extracted_json = get_json_from_queue(event)
    extracted_dict = convert_json_to_dict(extracted_json)
    dict_with_hashes = add_hashes(extracted_dict)
    transformed_dict = transform(dict_with_hashes)
    json_dict = json_serialize_dict(transformed_dict)
    send_json_to_queue(json_dict, TTOLQUEUE_URL)
    debug_prints(transformed_dict)
    return transformed_dict


def get_json_from_queue(event):
    return event["Records"][0]["body"]


def convert_json_to_dict(json_to_convert):
    generated_dict = json.loads(json_to_convert)
    print(generated_dict)
    return generated_dict


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


def json_serialize_dict(dict_):
    json_dict = json.dumps(dict_)

    return json_dict


def send_json_to_queue(json_dict, queue_url):
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
        MessageBody = json_dict
    )

    print(response['MessageId'])


################## TRANSFORM SECTION ###################
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
    cleaned_total_prices = [int(price.replace(".","")) for price in raw_list]
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


def add_hashes(dict_):
    entries_count = len(dict_["fname"])
    hashes = []
    
    for i in range(entries_count):
        string_to_hash = ""

        for list_ in dict_.values():
            string_to_hash += str(list_[i])

        hash_ = uuid.uuid3(uuid.NAMESPACE_OID, string_to_hash)
        hashes.append(hash_)

    dict_["hash"] = hashes

    return dict_


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

    print("Hashes of first 10 orders:")
    print(dict_["hash"][:10])

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