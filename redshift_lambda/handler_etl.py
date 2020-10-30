import psycopg2
import sys
import os
import csv
import boto3
import logging
import datetime 
from dotenv import load_dotenv
import re

#In the below code, "Order" refers to all info on one line (date, name, drinks purchased, total price etc).
#Whereas "Purchases" refer to just the drink information on the line. "Purchases" are one of several items in each "Order".

def start(event, context):
    print("Team One Pipeline")

    BUCKET_NAME = "cafe-data-data-pump-dev-team-1"
    SQL_TEXTFILE_KEY_NAME = "create_tables_postgresql.txt"

    load_dotenv()
    logging.getLogger().setLevel(0)
    
    file_to_extract = get_key_to_extract(event, BUCKET_NAME)

    if file_to_extract == None:
        return None

    conn = redshift_connect()
    extracted_dict, sql_code = extract(BUCKET_NAME, file_to_extract, SQL_TEXTFILE_KEY_NAME)
    transformed_dict = transform(extracted_dict)
    load(transformed_dict, conn, sql_code)
    return transformed_dict


def get_key_to_extract(event, bucket_name):
    keys = get_all_bucket_keys(bucket_name)

    if event["is_using_current_date"] == "True":
        key_to_extract = get_todays_key(keys)
    else:
        key_to_extract = keys[0]
        
    return key_to_extract


def get_todays_key(keys):
    raw_date_today = str(datetime.date.today())
    split_date_today = raw_date_today.split("-")
    date_today_correct_order = split_date_today[2] + "-" + split_date_today[1] + "-" + split_date_today[0]
    
    return_key = None
    
    for key in keys:
        key_date = key.split("_")[-2]
        
        if key_date == date_today_correct_order:
            return_key = key
            break
    
    return return_key

  
def redshift_connect():
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
            host=host
            )
        conn.set_session(autocommit = True)
        print("conn generation success")
    except Exception as ERROR:
        print("conn generation failure")
        print("Connection Issue: " + str(ERROR))
        sys.exit(1)

    print('connected')
    return conn

def get_all_bucket_keys(bucket_name):
    s3 = boto3.client('s3')
    object_list = s3.list_objects_v2(Bucket = bucket_name)
    contents = object_list["Contents"]
    
    key_names = []
    
    for key in contents:
        key_names.append(key["Key"])
        
    return key_names


def extract(bucket_name, cafe_csv_key_name, sql_textfile_name):
    raw_data, sql_code = read_from_s3(bucket_name, cafe_csv_key_name, sql_textfile_name)
    raw_lines = convert_data_to_lines(raw_data)
    comma_separated_lines = split_lines(raw_lines)
    clean_split_orders = remove_whitespace_and_quotes(comma_separated_lines)
    combined_purchase_orders = combine_purchases(clean_split_orders)
    
    dict_ = generate_dictionary(combined_purchase_orders)
    return (dict_, sql_code)


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

def load(cleaned_data, connection, sql_code_txtfile):
    
    create_database_tables(sql_code_txtfile, connection)

    insert_data_into_tables(cleaned_data, connection)

    return

###################### EXTRACT SECTION ##################
def read_from_s3(bucket, cafe_csv_key, sql_txtfile_key):
    s3 = boto3.client('s3')

    s3_raw_cafe_data = s3.get_object(Bucket = bucket, Key = cafe_csv_key)
    s3_sql_code = s3.get_object(Bucket = bucket, Key = sql_txtfile_key)

    data = s3_raw_cafe_data['Body'].read().decode('utf-8')
    sql_code = s3_sql_code['Body'].read().decode('utf-8')

    return (data, sql_code)


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


################## LOAD SECTION ################

def create_database_tables(sql_code_string, connection):
    """Arguments: filepath, database name. Programmatically populates specified database
     with tables using file containing SQL commands."""

    try:
        #Reformatting string to remove line breaks and tabs
        #re is an imported module for formatting
        reformatted_sql_string = re.sub(r"[\n\t]*", "", sql_code_string).strip()
        
        #creating list of strings each contaning SQL statement
        sql_string_list = reformatted_sql_string.split(";")
        
        #Executing each SQL statement 
        with connection.cursor() as cursor:
            for sql_command in sql_string_list:
                if type(sql_command) == str:
                    if sql_command.isspace() == False and len(sql_command) > 0:
                        print(sql_command)
                        cursor.execute(sql_command)
                        connection.commit()
            cursor.close()

    except Exception as e:
        print("Executing empty string")
        print(f"Exception Error: {e}")


#Function to convert any Python None values in the data to NULL (SQL only recognises NULL)
def convert_none_data_to_null(data):
    return list(convert_iterable_to_list_with_nones(data))


def convert_iterable_to_list_with_nones(iterable):
    iterable_type = type(iterable)
    list_with_nulls = []
    
    for item in iterable:
        if type(item) == tuple or type(item) == list:
            list_with_nulls.append(convert_iterable_to_list_with_nones(item))
        elif item == None:
            list_with_nulls.append("NULL")
        else:
            list_with_nulls.append(item)

    return iterable_type(list_with_nulls)
    

#Function checks if the tuple's chosen index has a None value in there or not    
def is_value_none(index, tuple_data):
    """Checks if in data (tuple) the specified index has a None value.
    If found to be True, returns True, otherwise False."""
    is_none = False
    if tuple_data[index] is None:
        is_none = True
    return is_none


#Function which given a datetime string, finds the corresponding day, month and year. 
#Returns out only all the unique days, months and years found from datetimes passed in
def corresponding_unique_days_months_years(datetimes):
    print("corresponding_unique_days_months_years")
    """Returns lists of unique days, months and years found from datetimes passed in"""
    datetime_objects = [datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S') for dtime in datetimes]
    
    days = [dtime.strftime("%A") for dtime in datetime_objects]
    unique_days = list(set(days))
    months = [dtime.strftime("%B") for dtime in datetime_objects]
    unique_months = list(set(months))
    years = [dtime.strftime("%Y") for dtime in datetime_objects]
    unique_years = list(set(years))
    return days, unique_days, months, unique_months, years, unique_years


#Function to reformat data from dictionary for data to be suitable for MySQL statements 
def reformatting_data_for_sql(data):
    print("reformatting_data_for_sql")
    """Extracts from input dictionary and reformats relevant data into a required format for MySQL statements. """
    
    first_names  = data["fname"]
    last_names = data["lname"]
    customer_names = list(zip(first_names, last_names))

    locations = data["location"]
    unique_locations = list(set(locations))

    datetimes = data["datetime"]
    days, unique_days, months, unique_months, years, unique_years = corresponding_unique_days_months_years(datetimes)

    total_prices = data["total_price"]
    payment_methods = data["payment_method"]
    card_numbers = data["card_number"]
    
    #For each dictionary in purchase list, joining location with drink ordered and drink info
    all_purchases = [list(zip(data["location"],purchase["drink_price"], purchase["drink_type"], purchase["drink_flavour"],purchase["drink_size"])) for purchase in data["purchase"]] 
    #OUTPUT FORMAT: all_purchases = [ [(location, Item_1 info) , (location, Item_7 info)], [(location, Item_6 info)] , ....]
    
    # For each item in a purchase, for each purchase in all_purchases, extracting item
    # and appending to all_items list
    all_items = [item for purchase in all_purchases for item in purchase]
    #OUTPUT FORMAT: all_items = [(item1),(item7),(item6),(item1)]
    unique_items = list(set(all_items))

    return datetimes, customer_names, unique_locations, days, unique_days, months, unique_months, years,unique_years, total_prices, payment_methods, card_numbers, unique_items, all_purchases, all_items


def insert_data_into_tables(data, connection):
    print("insert_data_into_tables")
    """Inserts data into various database tables"""
    try:
        with connection.cursor() as cursor:
            print("got cursor")
            #reformatted data suitable for MySQL statements
            datetimes, customer_names, unique_locations, days, unique_days,months, unique_months, years, unique_years, total_prices, payment_methods,card_numbers, unique_items, all_purchases, all_items = reformatting_data_for_sql(data)

            #insert data into tables in correct order due to dependencies (tier1 --> tier2 --> tier3)

            #tier 1
            print("inserting data into customer table")
            #inserting data into customer table
            sql_command_insert_data_into_table = 'INSERT INTO Customers (Forename, Surname) VALUES (%s, %s)'
            cursor.executemany(sql_command_insert_data_into_table, customer_names)
            cursor.commit()
            print("inserting data cafe location table")
            #inserting data into cafes locations table
            sql_command_insert_data_into_table = 'INSERT INTO Cafe_locations (Location_name) VALUES (%s)'
            cursor.executemany(sql_command_insert_data_into_table, unique_locations)
            cursor.commit()
            print("inserting data day;month;year tables")
            #inserting data into day, month, year tables
            sql_command_insert_data_into_table = "INSERT INTO Day (Day) VALUES (%s);"
            cursor.executemany( sql_command_insert_data_into_table, unique_days)
            cursor.commit()
            sql_command_insert_data_into_table = "INSERT INTO Month (Month) VALUES (%s);"
            cursor.executemany( sql_command_insert_data_into_table, unique_months)
            cursor.commit()
            sql_command_insert_data_into_table = "INSERT INTO Year (Year) VALUES (%s);"
            cursor.executemany( sql_command_insert_data_into_table, unique_years)
            cursor.commit()

            #tier 2

            #inserting data into payments table
            #selecting corresponding customer ids
            print("selecting corresponding customer ids")
            cursor.execute('SELECT c.Customer_id FROM Customers AS c')
            cursor.commit()

            #Extracts each row's customer_id but in tuple form: (customer_id, )
            print("Extracts each row's customer_id but in tuple form: (customer_id, )") 
            rows_of_customer_ids_tuples = cursor.FETCHALL()

            #Extracting customer_id out of tuple for each row and appending to customer_ids
            print("Extracting customer_id out of tuple for each row and appending to customer_ids") 
            customer_ids = [row[0] for row in rows_of_customer_ids_tuples]

            #reformatting data --> each customer_id with customer's payment info in a tuple
            print("reformatting data --> each customer_id with customer's payment info in a tuple") 
            payments_info = list(zip(customer_ids, total_prices, payment_methods, card_numbers))
            
            #inserting payment info data into payments table
            print("inserting payment info data into payments table") 
            sql_command_insert_data_into_table = """INSERT INTO Payments (Customer_id, Total_amount, Payment_type, Card_number) VALUES (%s, %s, %s,%s);"""
            cursor.executemany(sql_command_insert_data_into_table, convert_none_data_to_null(payments_info))
            cursor.commit()

            #Inserting data into table Time
            #First check datetimes corresponding day/month/year and then extract its id from tables 
            #Then id data insert into time table

            #Looping through days list which is 1-to-1 mapping with datetimes list
            #For datetimes corresponding day e.g. Monday, extracting its Day_id and
            #append to day_ids list
            print("append to day_ids list")
            day_ids = []
            for day in days:
                cursor.execute(f'SELECT d.Day_id FROM Day AS d WHERE d.Day = %s', day )
                cursor.commit()
                day_id = cursor.fetchone()[0]
                day_ids.append(day_id)

            #Looping through months list which is 1-to-1 mapping with datetimes list
            #For datetimes corresponding month e.g. July, extracting its Month_id and
            #append to month_ids list
            print("append to month_ids list")
            month_ids = []
            for month in months:
                cursor.execute(f'SELECT d.Month_id FROM Month AS d WHERE d.Month = %s', month )
                cursor.commit()
                month_id = cursor.fetchone()[0]
                month_ids.append(month_id)
            
            #Looping through years list which is 1-to-1 mapping with datetimes list
            #For datetimes corresponding day e.g. 2020, extracting its Year_id and
            #append to year_ids list
            print("append to year_ids list")
            year_ids = []
            for year in years:
                cursor.execute(f'SELECT d.Year_id FROM Year AS d WHERE d.Year = %s', year )
                cursor.commit()
                year_id = cursor.fetchone()[0]
                year_ids.append(year_id)
        
            #Joining lists to make a list of tuples
            print("Joining lists to make a list of tuples")
            full_datetimes_info = list(zip(datetimes, day_ids, month_ids, year_ids))
            #OUTPUT FORMAT: 
            #full_datetimes_info = [(datetime_1, day_id, month_id, year_id),
            #(datetime_2, day_id, month_id, year_id), ...]

            #List of only unique datetimes_info
            print("List of only unique datetimes_info")
            unique_datetimes =  list(set(full_datetimes_info))

            #inserting unique datetimes_info into table Time
            print("inserting unique datetimes_info into table Time")
            sql_command_insert_data_into_table = """INSERT INTO Time (datetime,Day_id,Month_id,Year_id) VALUES (STR_TO_DATE(%s, "%%Y-%%m-%%d %%H:%%i:%%S"), %s,%s,%s)"""
            cursor.executemany(sql_command_insert_data_into_table, unique_datetimes)
            cursor.commit()

            #Items table

            #Inserting data into Items table
            print("inserting data into Items table")
            sql_command_insert_data_into_table = """INSERT INTO Items (Location_name, Price , Drink_type , Drink_flavour, Drink_size) VALUES (%s, %s, %s,%s,%s)"""
            cursor.executemany(sql_command_insert_data_into_table, convert_none_data_to_null(unique_items))
            cursor.commit()
            
            #tier 3

            #Orders table

            #Looping through datetimes, which have 1-to-1 relationship with customer_ids/payment_ids
            #Selecting Time_id and appending to time_ids list
            print("Selecting Time_id and appending to time_ids list")
            time_ids = []
            for time in datetimes:
               cursor.execute("""SELECT t.Time_id From Time AS t WHERE t.datetime = %s""", time)
               cursor.commit()
               time_id = cursor.fetchone()[0]
               time_ids.append(time_id)

            #Looping through customer_names list
            #Selecting payment_id connected to a customer_id which is connected to customer name
            print("Selecting payment_id connected to a customer_id which is connected to customer name")
            payment_ids = []
            for name in customer_names:
                cursor.execute("""select p.Payment_id from Payments AS p join Customers AS c on c.Customer_id = p.Customer_id where c.forename = %s and c.surname = %s; """, name)
                cursor.commit()
                payment_id = cursor.fetchone()[0]
                payment_ids.append(payment_id)

            all_purchases_with_id = list(zip(payment_ids, convert_none_data_to_null(all_purchases), time_ids))
            
            #Selecting items_id for the corresponding payment_id

            print("Selecting items_id for the corresponding payment_id")
            item_ids = []
            orders_info = []

            for purchase in all_purchases_with_id:
                for drink_order in purchase[1]:

                    drink_flavour_index = 3
                    drink_size_index = 4
                    
                    if is_value_none(drink_flavour_index,drink_order) and is_value_none(drink_size_index, drink_order):
                        
                        drink_order_list = list(drink_order)

                        #removing index 3 and everything afterwards
                        del drink_order_list[3:]
                        
                        cursor.execute("""SELECT i.Item_id FROM  
                        Items AS i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour IS NULL
                        AND Drink_size IS NULL """, drink_order_list)
                        cursor.commit()
                        
                        item_id = cursor.fetchone()[0]
                        payment_id = purchase[0]
                        time_id = purchase[2]
                        
                        orders_info.append((payment_id, item_id,time_id))

                    elif is_value_none( drink_flavour_index, drink_order):
                        drink_order_list = list(drink_order)
                        drink_order_list.remove(drink_order_list[drink_flavour_index])
                        
                        cursor.execute("""SELECT i.Item_id FROM  
                        Items AS i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour IS NULL
                        AND Drink_size = %s """, drink_order_list)
                        cursor.commit()
                        
                        item_id = cursor.fetchone()[0]
                        payment_id = purchase[0]
                        time_id = purchase[2]
                        
                        orders_info.append((payment_id,item_id,time_id))

                    elif is_value_none(drink_size_index ,drink_order):
                        
                        drink_order_list = list(drink_order)
                        drink_order_list.remove(drink_order_list[drink_size_index])
                        
                        cursor.execute("""SELECT i.Item_id FROM  
                        Items AS i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour = %s
                        AND Drink_size IS NULL """, drink_order_list)
                        cursor.commit()
                        
                        time_id = cursor.fetchone()[0]
                        payment_id = purchase[0] 
                        time_id = purchase[2]
                        
                        orders_info.append((payment_id, item_id,time_id))
                    
                    else:
                        cursor.execute("""SELECT i.Item_id FROM  
                        Items AS i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour = %s
                        AND Drink_size= %s """, drink_order)
                        cursor.commit()
                        
                        item_id = cursor.fetchone()[0]
                        payment_id = purchase[0]
                        time_id = purchase[2]
                        
                        orders_info.append((payment_id,item_id,time_id))
            
            print("executing many")
            sql_command_insert_data_into_table = """INSERT INTO Orders (Payment_id, Item_id, Time_id) VALUES (%s, %s, %s)"""           
            cursor.executemany(sql_command_insert_data_into_table, orders_info)
            cursor.commit()
            cursor.close()
    # except Exception as e:
    #     #Rolls back any sql statements committed when error occurs partway to perserve data integrity
    #     connection.ROLLBACK()
    #     print(f"Exception Error: {e}")
    finally:
        connection.close()