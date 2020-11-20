import psycopg2
import sys
import os
import csv
import json
import boto3
import logging
import datetime
import uuid
from dotenv import load_dotenv
import re
import psycopg2.extras as psy


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

    transformed_jsons = get_json_from_queue(event)
    transformed_dicts = []

    for transformed_json in transformed_jsons:
        transformed_dict = convert_json_to_dict(transformed_json)
        print("type of transformed_dict returned by conversion:")
        print(type(transformed_dict))
        transformed_dicts.append(transformed_dict)
    
    combined_dict = combine_dicts(transformed_dicts)
    load(combined_dict, conn, sql_code)
    

def get_json_from_queue(event):
    records_list = []

    for record in event["Records"]:
        records_list.append(record["body"]) 

    return records_list


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


def combine_dicts(dict_list):
    print("number of dicts going into combine_dicts(): " + str(len(dict_list)))

    keys = dict_list[0].keys()
    combined_dict = {}

    for key in keys:
        combined_dict[key] = []

    for dict_ in dict_list:
        for key, list_ in dict_.items():
            combined_dict[key] += list_
            print(len(combined_dict[key]))
    
    return combined_dict


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
################ REFORMATTING DATA  #################################################################################
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
    if tuple_data[index] is 'NULL':
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

    times = [dtime.time().strftime('%H:%M:%S') for dtime in datetime_objects]
    
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
    hashed_payment_ids = data["hash"]

    #reformatting data --> In the list : For each customer creating a tuple with names, payment info, location and datetime
    print("reformatting data --> In the list : For each customer creating a tuple with names, payment info, location and datetime") 
    payments_info = list(zip(hashed_payment_ids, first_names, last_names, total_prices, payment_methods, card_numbers, locations, datetimes))
        
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

format_table_name = lambda name: name.replace("-", "")

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

        # print("Creating locations table copy")
        # create_locations_table_copy_sql_command = "CREATE TABLE Copy_cafe_locations AS SELECT * FROM Cafe_locations; TRUNCATE TABLE Cafe_locations;"
        # cursor.execute(create_locations_table_copy_sql_command)
        # connection.commit()
        # print("Committing cursor1")

        print("Truncating locations staging table")
        table_name = f'Staging_Cafe_locations_{format_table_name(str(uuid.uuid1()))}'
        sql_command_truncate_table = f"""
            CREATE TABLE {table_name}(
            Location_name varchar(100) NOT NULL,
            PRIMARY KEY(Location_name))
        """
        cursor.execute(sql_command_truncate_table)
        print("inserting data into locations table staging")
        sql_command_insert_data_into_table = f'INSERT INTO {table_name} (Location_name) VALUES %s'
        print("Using execute_values")
        psy.execute_values(cursor, sql_command_insert_data_into_table, unique_locations) 
        
        sql_command_insert_unique_data = f"""
        INSERT INTO Cafe_locations
        (SELECT Staging_Cafe_locations.Location_name
            FROM {table_name}
            LEFT OUTER JOIN Cafe_locations ON Cafe_locations.Location_name = Staging_Cafe_locations.Location_name
            WHERE Cafe_locations.Location_name IS NULL);
        """
        print("Executing inserting unique rows from staging table")
        cursor.execute(sql_command_insert_unique_data)
        cursor.execute(f"DROP TABLE {table_name}")
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

        print("Truncating Purchase_times staging table")
        table_name = f'Staging_Purchase_Times_{format_table_name(str(uuid.uuid1()))}'
        sql_command_truncate_table = f"""
            CREATE TABLE {table_name} (
            Datetime DATETIME,
            Day varchar(25),
            Month varchar(25),
            Year INT,
            Time TIME,
            PRIMARY KEY (Datetime))
        """
        cursor.execute(sql_command_truncate_table)
        print("inserting data into staging table for purchase_times")
        sql_command_insert_data_into_table = f'INSERT INTO {table_name} (Datetime, Day, Month, Year, Time) VALUES %s'
        print("Using execute_values")
        psy.execute_values(cursor, sql_command_insert_data_into_table, unique_full_datetimes_info, page_size=500)
        
        sql_command_insert_unique_data = f"""
        INSERT INTO Purchase_times
        (SELECT SPT.Datetime, SPT.Day, SPT.Month, SPT.Year, SPT.Time
            FROM {table_name} AS SPT
            LEFT OUTER JOIN Purchase_times 
            ON Purchase_times.Datetime = SPT.Datetime
            WHERE Purchase_times.Datetime IS NULL);
        """
        print("Executing inserting unique rows from staging table")
        cursor.execute(sql_command_insert_unique_data)
        cursor.execute(f"DROP TABLE {table_name}")
        print("Commiting")
        connection.commit()
        cursor.close()

def insert_data_into_payments_table(data, connection):
    print("insert_data_into_payments_tables")

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 2

        #Reformatted payment info 
        payments_info = reformat_payment_info_for_sql(data)
        print("Grabbing payment info")

        #inserting payment info data into staging payments table
        print("Truncating Staging_Payments staging table")
        table_name = f'Staging_Payments_{format_table_name(str(uuid.uuid1()))}'
        sql_command_truncate_table = f"""
            CREATE TABLE {table_name}(
            Payment_id VARCHAR,
            Forename varchar(100),
            Surname varchar(100),
            Total_amount INT,
            Payment_type VARCHAR(100),
            Card_number VARCHAR(255),
            Location_name VARCHAR,
            Datetime DATETIME,
            PRIMARY KEY(Payment_id))
        """
        cursor.execute(sql_command_truncate_table)        
        print("inserting data into staging table for Payments")
        sql_command_insert_data_into_table = f'INSERT INTO {table_name} (Payment_id, Forename, Surname, Total_amount, Payment_type, Card_number, Location_name, Datetime) VALUES %s'
        print("Using execute_values")
        psy.execute_values(cursor, sql_command_insert_data_into_table, payments_info, page_size=500)
        
        sql_command_insert_unique_data = f"""
        INSERT INTO Payments
        (SELECT SP.Payment_id, SP.Forename, SP.Surname, SP.Total_amount, SP.Payment_type, SP.Card_number, SP.Location_name, SP.Datetime
            FROM {table_name} AS SP
            LEFT OUTER JOIN Payments AS P
            ON P.Payment_id = SP.Payment_id
            WHERE P.Payment_id IS NULL);
        """
        print("Executing inserting unique rows from staging table")
        cursor.execute(sql_command_insert_unique_data)
        cursor.execute(f"DROP TABLE {table_name}")
        connection.commit()
        cursor.close()
        
    
        # print("inserting payment info data into payments table") 
        # sql_command_insert_data_into_table = """INSERT INTO Payments (Payment_id, Forename, Surname, Total_amount, Payment_type, Card_number, Location_name, Datetime) VALUES %s ;"""
        # psy.execute_values(cursor, sql_command_insert_data_into_table, convert_none_data_to_null(payments_info))
        # connection.commit()    
        # cursor.close()


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

        
        #Inserting data into Staging Items table
        print("Truncating Staging_Items staging table")
        table_name = f'Staging_Items_{format_table_name(str(uuid.uuid1()))}'
        sql_command_truncate_table = f"""
            CREATE TABLE IF {table_name} (
            Item_id INT IDENTITY(1,1),
            Price INT,
            Drink_type VARCHAR(100),
            Drink_flavour VARCHAR(100),
            Drink_size VARCHAR(100),
            PRIMARY KEY (Item_id))
        """
        cursor.execute(sql_command_truncate_table) 
        print("inserting data into staging table for Items")
        sql_command_insert_data_into_table = f'INSERT INTO {table_name} (Drink_type, Drink_flavour, Drink_size, Price) VALUES %s'
        print("Using execute_values")
        psy.execute_values(cursor, sql_command_insert_data_into_table, unique_items, page_size=500)
        
        sql_command_insert_unique_data = f"""
        INSERT INTO Items (Price, Drink_type, Drink_flavour, Drink_size)
        (SELECT SI.price, SI.drink_type, SI.drink_flavour, SI.drink_size
            FROM {table_name} AS SI
            LEFT OUTER JOIN Items AS I
            ON I.drink_type = SI.drink_type
            AND I.price =  SI.price
            AND (I.drink_flavour = SI.drink_flavour OR (I.drink_flavour IS NULL AND SI.drink_flavour IS NULL))
            AND (I.drink_size = SI.drink_size OR (I.drink_size IS NULL AND SI.drink_size IS NULL))
            WHERE I.Drink_type IS NULL
            AND I.Drink_flavour IS NULL
            AND I.Drink_size IS NULL
            AND I.Price IS NULL);
        """
        print("Executing inserting unique rows from staging table")
        cursor.execute(sql_command_insert_unique_data)
        cursor.execute(f"DROP TABLE {table_name}")
        connection.commit()
        cursor.close()



        # print("Inserting data into Items table")
        # sql_command_insert_data_into_table = """INSERT INTO Items (Drink_type , Drink_flavour, Drink_size,Price) VALUES %s """
        # psy.execute_values(cursor, sql_command_insert_data_into_table, convert_none_data_to_null(unique_items))
        # connection.commit()
        # cursor.close()


#####################################################################################################################################
#####################################################################################################################################
#####################################################################################################################################
#####################################################################################################################################


def insert_data_into_orders_table(data, connection):
    print("insert_data_into_orders_tables")
    """Inserts data into orders table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 3

        #Fetching required data to be used
  
        hashed_payment_ids = data["hash"]
        all_purchases = reformat_items_info_for_sql(data, "ALL_PURCHASES")
        # [  [(drink1, drink1price), (drink2)] , [person2 ], ...   ]

        print("Grabbing hashed_payment_ids/payment_info/purchase_info")

        #Orders table
        #Each payment id joined with purchase (which is a list)
        print("Joining payment_id with purchase_info for each purchase made")
        all_purchases_with_payment_id = list(zip(hashed_payment_ids, convert_none_data_to_null(all_purchases)))
        
        #Selecting items_id for the corresponding payment_id 

        print("Selecting items_id for the corresponding payment_id")
        orders_info = []

        print("looping through each drink in each purchase")
        for purchase in all_purchases_with_payment_id:
            # TODO: We are currently grabbing menu item IDs from the table one by one for each drink in each order (eg. 400 * 2?)
            # 1. Given then that items are menu items is the table expected to grow beyond a size where pulling the IDs for the whole table
            # will impact perforance? If not, we could grab the whole table in one go here and then match drinks to IDs in python
            # One downside is you need the whole table to match properties to drinks.
            # 2. Would calculating menu item ids upfront instead of auto increment also work here - only an option as you may go this route
            # for payment_ids?

            for drink_order in purchase[1]:

                drink_flavour_index = 1
                drink_size_index = 2
                
                if is_value_none(drink_flavour_index,drink_order) and is_value_none(drink_size_index, drink_order):
                    
                    drink_order_list = list(drink_order)

                    #removing index 1 and index 2
                    #note index 3 is not inclusive so it is not removed
                    del drink_order_list[1:3]
                    
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE i.Drink_type = %s AND i.Drink_flavour IS NULL
                    AND i.Drink_size IS NULL AND i.Price = %s""", drink_order_list)
                    connection.commit()
                    
                    #print(f"drink_order_list: {drink_order_list}")
                    #print("sql worked for selected id")
                    
                    item_id = cursor.fetchone()[0]
                    print(f"fetching item_id: {item_id}")
                    payment_id = purchase[0]
                    
                    orders_info.append((payment_id, item_id))

                elif is_value_none( drink_flavour_index, drink_order):
                    drink_order_list = list(drink_order)
                    drink_order_list.remove(drink_order_list[drink_flavour_index])

                    
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE i.Drink_type = %s AND i.Drink_flavour IS NULL
                    AND i.Drink_size = %s AND i.Price = %s""", drink_order_list)
                    connection.commit()
                    #print(f"drink_order_list: {drink_order_list}") 
                    #print("sql worked for selected id")
                    
                    item_id = cursor.fetchone()[0]
                    print(f"fetching item_id: {item_id}")
                    payment_id = purchase[0]
                    
                    orders_info.append((payment_id,item_id))

                elif is_value_none(drink_size_index ,drink_order):
                    
                    drink_order_list = list(drink_order)
                    drink_order_list.remove(drink_order_list[drink_size_index])

                    
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE i.Drink_type = %s AND i.Drink_flavour = %s
                    AND i.Drink_size IS NULL AND i.Price = %s""", drink_order_list)
                    connection.commit()
                    #print(f"drink_order_list: {drink_order_list}")
                    #print("sql worked for selected id")

                    
                    item_id = cursor.fetchone()[0]
                    print(f"fetching item_id: {item_id}")
                    payment_id = purchase[0] 
                    
                    orders_info.append((payment_id, item_id))
                
                else:
                    
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE  i.Drink_type = %s AND i.Drink_flavour = %s
                    AND i.Drink_size = %s AND i.Price = %s""", drink_order)
                    connection.commit()
                    #print(f"drink_order: {drink_order}")
                    #print("sql worked for selected id")
                    
                    item_id = cursor.fetchone()[0]
                    print(f"fetching item_id: {item_id}")
                    payment_id = purchase[0]
                    
                    orders_info.append((payment_id,item_id))

        print("Truncating Staging_Orders staging table")
        # sql_command_truncate_table = "TRUNCATE TABLE Staging_Orders"
        table_name = f'Staging_Orders_{format_table_name(str(uuid.uuid1()))}'
        sql_command_truncate_table = f"""
            CREATE TABLE {table_name}(
                Order_id INT IDENTITY(1, 1),
                Item_id INT,
                Payment_id VARCHAR,
                PRIMARY KEY(Order_id))
        """
        cursor.execute(sql_command_truncate_table)            
        print("inserting data into staging table for Orders")
        sql_command_insert_data_into_table = f'INSERT INTO {table_name} (Payment_id, Item_id) VALUES %s'
        print("Using execute_values")
        psy.execute_values(cursor, sql_command_insert_data_into_table, orders_info, page_size=500)
        
        sql_command_insert_unique_data = f"""
        INSERT INTO Orders (Item_id, Payment_id)
        (SELECT SO.Item_id, SO.Payment_id
            FROM {table_name} AS SO
            LEFT OUTER JOIN Orders AS O
            ON O.Payment_id = SO.Payment_id
            AND O.Item_id = SO.Item_id
            WHERE O.Item_id IS NULL
            AND O.Payment_id IS NULL);
        """
        print("Executing inserting unique rows from staging table")
        cursor.execute(sql_command_insert_unique_data)
        cursor.execute(f'DROP TABLE {table_name}')
        connection.commit()
        cursor.close()


def insert_data_into_all_tables(data, connection):
    print("insert_data_into_all_tables")
    """Inserts data into various database tables"""
    try:
        insert_data_cafe_locations_table(data, connection)
        insert_data_into_purchase_times_table(data, connection)
        insert_data_into_payments_table(data, connection)
        insert_data_into_items_table(data,connection)
        insert_data_into_orders_table(data,connection)
        print("insert_data_into_all_tables success")
    except Exception as e:
        print("insert_data_into_all_tables failure")
        print(e)
    finally:
        connection.close()
