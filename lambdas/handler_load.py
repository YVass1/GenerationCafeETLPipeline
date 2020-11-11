import psycopg2
import sys
import os
import csv
import json
import boto3
import logging
import datetime 
from dotenv import load_dotenv
import re

def start(event, context):
    print("Team One Pipeline")
    
    BUCKET_NAME = "cafe-data-data-pump-dev-team-1"
    SQL_TEXTFILE_KEY_NAME = "tables_creation_sql_code.txt"

    sql_code = read_from_s3(BUCKET_NAME, SQL_TEXTFILE_KEY_NAME)

    load_dotenv()
    conn = redshift_connect()
    logging.getLogger().setLevel(0)

    print(event)
    print(context)

    transformed_json = get_json_from_queue(event)
    transformed_dict = convert_json_to_dict(transformed_json)

    load(transformed_dict, conn, sql_code)
    

def get_json_from_queue(event):
    return event["Records"][0]["body"]


def convert_json_to_dict(json_to_convert):
    generated_dict = json.loads(json_to_convert)
    print(generated_dict)
    return generated_dict


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


def read_from_s3(bucket, sql_txtfile_key):
    s3 = boto3.client('s3')

    s3_sql_code = s3.get_object(Bucket = bucket, Key = sql_txtfile_key)

    sql_code = s3_sql_code['Body'].read().decode('utf-8')

    return (sql_code)


def load(cleaned_data, connection, sql_code_txtfile):
    
    create_database_tables(sql_code_txtfile, connection)

    insert_data_into_all_tables(cleaned_data, connection)


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
        print(f"Exception Error: {e}")


#####################################################################################################################
#####################################################################################################################
################ REFORMATTING DATA  #########################################################################
#####################################################################################################################
#####################################################################################################################

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


#Function which given a datetime string, finds the corresponding day, month, year and time. 

def reformat_datetime_info_for_sql(data):
    """Extracts datetime info from input dictionary and reformats into a required format for MySQL statements.
    Returns lists of days, months, years and times found from datetimes passed in."""
    print("reformatting_datetimes_data_for_sql")
    
    datetimes = data["datetime"]

    datetime_objects = [datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S') for dtime in datetimes]
    
    days = [dtime.strftime("%A") for dtime in datetime_objects]
    
    months = [dtime.strftime("%B") for dtime in datetime_objects]
    
    years = [dtime.strftime("%Y") for dtime in datetime_objects]

    #Temporarily using the full timestamp as the time. To extract just time, would use the code: times = [dtime.time().strftime('%H:%M:%S') for dtime in datetime_objects]
    times = data["datetime"]
    
    return datetimes, days, months, years, times


#Functions to reformat data from dictionary for data to be suitable for MySQL statements 

def reformat_cafe_locations_info_for_sql(data):
    """Extracts cafe locations info from input dictionary and reformats into a required format for MySQL statements."""
    print("reformatting_cafe_locations_data_for_sql")
    locations = data["location"]
    location_set = list(set(locations))
    unique_locations = []

    for loc in location_set:
        unique_locations.append((loc,))
    
    return unique_locations


def reformat_payment_info_for_sql(data):
    """Extracts payment info from input dictionary and reformats into a required format for MySQL statements."""
    print("reformatting_payment_data_for_sql")

    first_names  = data["fname"]
    last_names = data["lname"]
    total_prices = data["total_price"]
    payment_methods = data["payment_method"]
    card_numbers = data["card_number"]
    locations = data["location"]
    datetimes = data["datetime"]

    #reformatting data --> In the list : For each customer creating a tuple with names, payment info, location and datetime
    print("reformatting data --> In the list : For each customer creating a tuple with names, payment info, location and datetime") 
    payments_info = list(zip(first_names, last_names, total_prices, payment_methods, card_numbers, locations, datetimes))
        
    return payments_info


def reformat_items_info_for_sql(data, return_type = "ALL"):
    """Reformats purchases info from input dictionary and reformats into a required format for MySQL statements."""
    print("reformat_items_info_for_sql")
    
    #For each dictionary in purchase list, joining drink ordered with drink size, flavour, price 
    all_purchases = [list(zip(purchase["drink_type"], purchase["drink_flavour"], purchase["drink_size"], purchase["drink_price"])) for purchase in data["purchase"]] 
    #OUTPUT FORMAT: all_purchases = [ [(drink1, flavour1, size1, price1), (drink6, flavour6, size6, price6)], [(drink9, flavour9, size9, price9)] , ....]
    
    # For each item in a purchase, for each purchase in all_purchases, extracting item
    # and appending to all_items list
    all_items = [item for purchase in all_purchases for item in purchase]
    print("Reformat function: printing all items")
    print(all_items)
    #OUTPUT FORMAT: all_items = [(item1_info),(item7_info),(item6_info),(item8_info)]
    
    #ensuring unique items only in the list
    unique_items = list(set(all_items))
    print(unique_items)

    if return_type == "ALL_PURCHASES":
        return all_purchases
    elif return_type == "ALL_ITEMS":
        return all_items
    elif return_type == "UNIQUE_ITEMS":
        return unique_items
    elif return_type == "ALL":
        return unique_items, all_purchases, all_items
    else:
        print("FAIL")

#####################################################################################################################
#####################################################################################################################
################ INSERTING DATA INTO TABLES #########################################################################
#####################################################################################################################
#####################################################################################################################

#insert data into tables in correct order due to dependencies (tier1 --> tier2 --> tier3)


def insert_data_cafe_locations_table(data, connection):
    print("insert_data_into_cafe_locations_table")
    """Inserts data into cafe locations table"""

    with connection.cursor() as cursor:
        print("got cursor")

        #Tier 1

        #reformatted data suitable for MySQL statements
        unique_locations = reformat_cafe_locations_info_for_sql(data)
        print(unique_locations)
        print("Grabbing reformatted cafe locations data")

        print("Inserting data into cafe locations table")
        #inserting data into cafes locations table
        
        #Ongoing work for duplicates
        # other option: 
        #CREATE TEMP TABLE staging_table (LIKE target_table); staging table should have new data
        #INSERT INTO staging_table;
        #DELETE FROM staging_table USING target_table WHERE staging_table.Cafe_locations = target_table.Cafe_locations;
        #INSERT INTO target_table SELECT * FROM staging_table;
        #         #Ongoing duplicates work:
        #CREATE TEMP TABLE Staging_Cafe_locations AS SELECT * FROM Cafe_locations;

        #DELETE FROM staging_table USING target_table WHERE staging_table.Cafe_locations = target_table.Cafe_locations;
        #INSERT INTO target_table SELECT * FROM staging_table;
        #create_location_staging_table = "CREATE TABLE Staging_Cafe_locations AS SELECT * FROM Cafe_locations;"
        #cursor.execute(create_location_staging_table)
        #connection.commit()
        
        sql_command_insert_data_into_table = 'INSERT INTO Cafe_locations (Location_name) VALUES (%s)'
        cursor.executemany(sql_command_insert_data_into_table, unique_locations)
        connection.commit()
        cursor.close()

def insert_data_into_purchase_times_table(data, connection):
    print("insert_data_into_purchase_times_table")
    """Inserts data into full datetime table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 1

        #Reformatted datetime data
        datetimes, days, months, years, times =  reformat_datetime_info_for_sql(data)
        print("Successfully grabbed datetimes data")
    
        #Joining lists to make a list of tuples with correct data:
        #Data format wanted: [(datetime1, Day1, Month1, Year1, Time1),
        #(datetime2, Day2, Month2, Year2, Time2), ...]
        print("Joining lists to make a list of tuples")

        full_datetimes_info = list(zip(datetimes, days, months, years, times))
        #OUTPUT FORMAT: 

        #List of only unique full_datetimes_info
        print("List of only unique datetimes_info")
        unique_full_datetimes_info =  list(set(full_datetimes_info))

        #inserting unique full_datetimes_info into table Purchase_times table
        print("inserting unique datetimes_info into table Purchase_times")
        sql_command_insert_data_into_table = """INSERT INTO Purchase_times (Datetime, Day, Month, Year, Time) VALUES (%s, %s, %s, %s, %s) """
        cursor.executemany(sql_command_insert_data_into_table, unique_full_datetimes_info)
        connection.commit()
        cursor.close()

def insert_data_into_payments_table(data, connection):
    print("insert_data_into_payments_tables")
    """Inserts data into payments table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 2

        #Reformatted payment info 
        payments_info = reformat_payment_info_for_sql(data)
        print("Grabbing payment info")

        #inserting payment info data into payments table
        print("inserting payment info data into payments table") 
        sql_command_insert_data_into_table = """INSERT INTO Payments (Forename, Surname, Total_amount, Payment_type, Card_number, Location_name, Datetime) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        cursor.executemany(sql_command_insert_data_into_table, convert_none_data_to_null(payments_info))
        connection.commit()
        
        #Extracting recently inserted payment ids
        number_of_rows_inserted = len(payments_info)
        sql_command_select_payment_ids = f'SELECT p.Payment_id FROM Payments AS p ORDER BY p.Payment_id DESC LIMIT {number_of_rows_inserted}'
        cursor.execute(sql_command_select_payment_ids)

        payment_ids_tuple_list = cursor.fetchall()
        print("Fetching payment_ids tuple")
        
        payment_ids_list = []
        for tup in payment_ids_tuple_list:
            payment_ids_list.insert(0, tup[0]) #inserting at 0 constantly will invert the element order, since elements are grabbed in descending order from database
        print("Extracting payment_id from tuple")

        data["payment_id"] = payment_ids_list
        
        cursor.close()


def insert_data_into_items_table(data, connection):
    print("insert_data_into_items_tables")
    """Inserts data into items table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 2

        #Reformatted payment info 
        unique_items = reformat_items_info_for_sql(data, "UNIQUE_ITEMS")
        print("Grabbing purchases/items info")

        #Items table

        #Inserting data into Items table
        print("Inserting data into Items table")
        sql_command_insert_data_into_table = """INSERT INTO Items (Drink_type , Drink_flavour, Drink_size,Price) VALUES (%s, %s, %s, %s) """
        cursor.executemany(sql_command_insert_data_into_table, convert_none_data_to_null(unique_items))
        connection.commit()
        cursor.close()


def insert_data_into_orders_table(data, connection):
    print("insert_data_into_orders_tables")
    """Inserts data into orders table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 3

        #Fetching required data to be used
  
        datetimes =  data["datetime"]
        payment_ids = data["payment_id"]
        payments_info = reformat_payment_info_for_sql(data)
        all_purchases = reformat_items_info_for_sql(data, "ALL_PURCHASES")

        print("Grabbing payment_ids/payment_info/purchase_info")

        #Orders table
        #Each payment id joined with purchase (which is a list)
        print("Joining payment_id with purchase_info for each purchase made")
        all_purchases_with_payment_id = list(zip(payment_ids, convert_none_data_to_null(all_purchases)))
        
        #Selecting items_id for the corresponding payment_id 

        print("Selecting items_id for the corresponding payment_id")
        item_ids = []
        orders_info = []


        for purchase in all_purchases_with_payment_id:
            print(f"Length of 'all_purchases_with_payment_id' list: {len(all_purchases_with_payment_id)}")
            print(f"Length of purchase[1] for this purchase: {len(purchase[1])}")
            for drink_order in purchase[1]:

                drink_flavour_index = 1
                drink_size_index = 2
                
                if is_value_none(drink_flavour_index,drink_order) and is_value_none(drink_size_index, drink_order):
                    
                    drink_order_list = list(drink_order)

                    #removing index 1 and index 2
                    #note index 3 is not inclusive so it is not removed
                    del drink_order_list[1:3]
                    
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE  i.Drink_type = %s AND i.Drink_flavour IS NULL
                    AND Drink_size IS NULL AND i.Price = %s """, drink_order_list)
                    connection.commit()
                    
                    item_id = cursor.fetchone()[0]
                    payment_id = purchase[0]
                    
                    orders_info.append((payment_id, item_id))

                elif is_value_none( drink_flavour_index, drink_order):
                    drink_order_list = list(drink_order)
                    drink_order_list.remove(drink_order_list[drink_flavour_index])
                    
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE i.Drink_type = %s AND i.Drink_flavour IS NULL
                    AND Drink_size = %s AND i.Price = %s""", drink_order_list)
                    connection.commit()
                    
                    item_id = cursor.fetchone()[0]
                    payment_id = purchase[0]
                    
                    orders_info.append((payment_id,item_id))

                elif is_value_none(drink_size_index ,drink_order):
                    
                    drink_order_list = list(drink_order)
                    drink_order_list.remove(drink_order_list[drink_size_index])
                    
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE i.Drink_type = %s AND i.Drink_flavour = %s
                    AND Drink_size IS NULL AND i.Price = %s""", drink_order_list)
                    connection.commit()
                    
                    item_id = cursor.fetchone()[0]
                    payment_id = purchase[0] 
                    
                    orders_info.append((payment_id, item_id))
                
                else:
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE  i.Drink_type = %s AND i.Drink_flavour = %s
                    AND Drink_size= %s AND i.Price = %s """, drink_order)
                    connection.commit()
                    
                    item_id = cursor.fetchone()[0]
                    payment_id = purchase[0]
                    
                    orders_info.append((payment_id,item_id))
                
        print("Printing orders info: list of tuples with 2 arguments")
        print(orders_info)
                    
        
        print("executing many")
        sql_command_insert_data_into_table = "INSERT INTO Orders (Payment_id, Item_id) VALUES (%s, %s)"  
        print("sql command variable")         
        cursor.executemany(sql_command_insert_data_into_table, orders_info)
        print("excute many for inserting into orders")
        connection.commit()
        print("connection being committed")
        cursor.close()
        print("cursor closed")


def insert_data_into_all_tables(data, connection):
    print("insert_data_into_all_tables")
    """Inserts data into various database tables"""
    try:
        insert_data_cafe_locations_table(data, connection)
        insert_data_into_purchase_times_table(data, connection)
        insert_data_into_payments_table(data, connection)
        insert_data_into_items_table(data,connection)
        insert_data_into_orders_table(data,connection)
    except Exception as e:
        print("insert_data_into_all_tables failure")
        print(e)
    finally:
        connection.close()