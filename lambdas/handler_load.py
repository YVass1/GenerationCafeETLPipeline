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
    SQL_TEXTFILE_KEY_NAME = "create_tables_postgresql.txt"

    sql_code = read_from_s3(BUCKET_NAME, SQL_TEXTFILE_KEY_NAME)

    load_dotenv()
    conn = redshift_connect()
    logging.getLogger().setLevel(0)

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

    unique_days_as_tuple_list = []
    unique_months_as_tuple_list = []
    unique_years_as_tuple_list = []

    for day in unique_days:
        unique_days_as_tuple_list.append((day, ))
    for month in unique_months:
        unique_months_as_tuple_list.append((month, ))
    for year in unique_years:
        unique_years_as_tuple_list.append((year,))

    return days, unique_days_as_tuple_list, months, unique_months_as_tuple_list, years, unique_years_as_tuple_list


#Functions to reformat data from dictionary for data to be suitable for MySQL statements 
def reformat_customer_info_for_sql(data):
    """Extracts customer info from input dictionary and reformats into a required format for MySQL statements."""
    print("reformatting_customer_data_for_sql")
    first_names  = data["fname"]
    last_names = data["lname"]
    customer_names = list(zip(first_names, last_names))

    return customer_names


def reformat_cafe_locations_info_for_sql(data):
    """Extracts cafe locations info from input dictionary and reformats into a required format for MySQL statements."""
    print("reformatting_cafe_locations_data_for_sql")
    locations = data["location"]
    location_set = list(set(locations))
    unique_locations = []

    for loc in location_set:
        unique_locations.append((loc,))
    
    return unique_locations


def reformat_datetime_info_for_sql(data, return_type = "ALL" ):
    """Extracts datetime info from input dictionary and reformats into a required format for MySQL statements."""
    print("reformatting_datetimes_data_for_sql")
    datetimes = data["datetime"]
    days, unique_days, months, unique_months, years, unique_years = corresponding_unique_days_months_years(datetimes)
    
    if return_type == "UNIQUE":
        return unique_days, unique_months, unique_years
    elif return_type == "NON-UNIQUE":
       return days, months, years
    elif return_type == "ALL":
        return datetimes, days, unique_days, months, unique_months, years,unique_years


def reformat_payment_info_for_sql(data):
    """Extracts payment info from input dictionary and reformats into a required format for MySQL statements."""
    print("reformatting_payment_data_for_sql")

    total_prices = data["total_price"]
    payment_methods = data["payment_method"]
    card_numbers = data["card_number"]

    return total_prices, payment_methods, card_numbers


def reformat_purchases_info_for_sql(data, return_type = "ALL"):
    """Reformats purchases info from input dictionary and reformats into a required format for MySQL statements."""
    print("reformat_purchases_info_for_sql")
    
    #For each dictionary in purchase list, joining location with drink ordered and drink info
    all_purchases = [list(zip(data["location"],purchase["drink_price"], purchase["drink_type"], purchase["drink_flavour"],purchase["drink_size"])) for purchase in data["purchase"]] 
    #OUTPUT FORMAT: all_purchases = [ [(location, Item_1 info) , (location, Item_7 info)], [(location, Item_6 info)] , ....]
    
    # For each item in a purchase, for each purchase in all_purchases, extracting item
    # and appending to all_items list
    all_items = [item for purchase in all_purchases for item in purchase]
    #OUTPUT FORMAT: all_items = [(item1_info),(item7),(item6),(item1)]
    
    #all_items_for_duplicate_check = list(zip(all_items,all_items))
    unique_items = list(set(all_items))

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


#insert data into tables in correct order due to dependencies (tier1 --> tier2 --> tier3)
def insert_data_into_customer_table(data,connection):
    print("insert_data_into_customer_table")
    """Inserts data into customer table"""
    with connection.cursor() as cursor:
        print("got cursor")

        #Tier 1

        #reformatted data suitable for MySQL statements
        customer_names = reformat_customer_info_for_sql(data)
        print("Grabbing reformatted customer info")
        print(customer_names)
        customer_ids_list = []
        
        #inserting data into customer table
        print("Inserting customer info data into table")
        sql_command_insert_data_into_table = 'INSERT INTO Customers (Forename, Surname) VALUES (%s, %s)'
        for name in customer_names:
            cursor.execute(sql_command_insert_data_into_table, name)
            connection.commit()
            sql_command_select_customer_id = 'SELECT c.Customer_id FROM Customers AS c WHERE c.Forename = %s AND c.Surname = %s AND c.Customer_id = MAX(c.Customer_id)'
            cursor.execute(sql_command_select_customer_id, name)
            customer_id = cursor.fetchone()[0]
            customer_ids_list.append(customer_id)
        
        cursor.close()

        data["Customer_id"] = customer_ids_list


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
        #create_location_staging_table = "CREATE TABLE Cafe_locations_update AS SELECT * FROM Cafe_locations;"
        #cursor.execute(create_location_staging_table)
        #connection.commit()
        
        sql_command_insert_data_into_table = 'INSERT INTO Cafe_locations (Location_name) VALUES (%s)'
        cursor.executemany(sql_command_insert_data_into_table, unique_locations)
        connection.commit()
        cursor.close()


def insert_data_into_day_month_year_tables(data, connection):
    print("insert_data_into_day;month;year_tables")
    """Inserts data into day;month;year tables"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 1

        #reformatted data suitable for MySQL statements
        unique_days, unique_months, unique_years = reformat_datetime_info_for_sql(data, "UNIQUE")
        print("Grabbing reformatted day;month;year data")

        print("Inserting data day;month;year tables")
        #inserting data into day, month, year tables
        sql_command_insert_data_into_table = "INSERT INTO Day (Day) VALUES (%s);"
        cursor.executemany( sql_command_insert_data_into_table, unique_days)
        connection.commit()
        sql_command_insert_data_into_table = "INSERT INTO Month (Month) VALUES (%s);"
        cursor.executemany( sql_command_insert_data_into_table, unique_months)
        connection.commit()
        sql_command_insert_data_into_table = "INSERT INTO Year (Year) VALUES (%s);"
        cursor.executemany( sql_command_insert_data_into_table, unique_years)
        connection.commit()
        cursor.close()


def insert_data_into_payments_table(data,connection):
    print("insert_data_into_payments_tables")
    """Inserts data into payments table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 2

        #Reformatted payment info 
        total_prices, payment_methods, card_numbers = reformat_payment_info_for_sql(data)
        print("Grabbing payment info")

        #Reformatted customer ids info 
        customer_ids_list = data["Customer_id"]
        print("Grabbing customer_ids_list info")

        #inserting data into payments table

        #reformatting data --> each customer_id with customer's payment info in a tuple
        print("reformatting data --> each customer_id with customer's payment info in a tuple") 
        payments_info = list(zip(customer_ids_list, total_prices, payment_methods, card_numbers))
        
        #inserting payment info data into payments table
        print("inserting payment info data into payments table") 
        sql_command_insert_data_into_table = """INSERT INTO Payments (Customer_id, Total_amount, Payment_type, Card_number) VALUES (%s, %s, %s,%s);"""
        cursor.executemany(sql_command_insert_data_into_table, convert_none_data_to_null(payments_info))
        connection.commit()
        cursor.close()


def insert_data_into_full_datetime_table(data,connection):
    print("insert_data_into_full_datetime_tables")
    """Inserts data into full datetime table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 2

        #Reformatted datetime data
        datetimes = data["datatime"]
        days, months, years =  reformat_datetime_info_for_sql(data, "NON-UNIQUE")
        print("Successfully grabbed datetimes data")

        #Inserting data into table Time
        #First check datetimes corresponding day/month/year and then extract its id from tables 
        #Then id data insert into time table

        #Looping through days list which is 1-to-1 mapping with datetimes list
        #For datetimes corresponding day e.g. Monday, extracting its Day_id and
        #append to day_ids list
        print("append to day_ids list")
        day_ids = []
        for day in days:
            cursor.execute('SELECT d.Day_id FROM Day AS d WHERE d.Day = %s', (day, ) )
            connection.commit()
            day_id = cursor.fetchone()[0]
            day_ids.append(day_id)

        #Looping through months list which is 1-to-1 mapping with datetimes list
        #For datetimes corresponding month e.g. July, extracting its Month_id and
        #append to month_ids list
        print("append to month_ids list")
        month_ids = []
        for month in months:
            cursor.execute('SELECT d.Month_id FROM Month AS d WHERE d.Month = %s', (month, ) )
            connection.commit()
            month_id = cursor.fetchone()[0]
            month_ids.append(month_id)
        
        #Looping through years list which is 1-to-1 mapping with datetimes list
        #For datetimes corresponding day e.g. 2020, extracting its Year_id and
        #append to year_ids list
        print("append to year_ids list")
        year_ids = []
        for year in years:
            cursor.execute('SELECT d.Year_id FROM Year AS d WHERE d.Year = %s', (year, ) )
            connection.commit()
            year_id = cursor.fetchone()[0]
            year_ids.append(year_id)
    
        #Joining lists to make a list of tuples
        print("Joining lists to make a list of tuples")
        full_datetimes_info = list(zip(datetimes, day_ids, month_ids, year_ids))
        #OUTPUT FORMAT: 
        #full_datetimes_info = [(datetime_1, day_id, month_id, year_id),
        #(datetime_2, day_id, month_id, year_id), ...]

        #full_datetimes_info_for_duplicate_check = list(zip(full_datetimes_info, full_datetimes_info))

        #List of only unique datetimes_info
        print("List of only unique datetimes_info")
        unique_datetimes =  list(set(full_datetimes_info))

        #inserting unique datetimes_info into table Time
        print("inserting unique datetimes_info into table Time")
        sql_command_insert_data_into_table = """INSERT INTO Time (datetime,Day_id,Month_id,Year_id) VALUES (%s, %s,%s,%s) """
        cursor.executemany(sql_command_insert_data_into_table, unique_datetimes)
        connection.commit()
        cursor.close()


def insert_data_into_items_table(data,connection):
    print("insert_data_into_items_tables")
    """Inserts data into items table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 2

        #Reformatted payment info 
        unique_items = reformat_purchases_info_for_sql(data, "UNIQUE_ITEMS")
        print("Grabbing purchases/items info")

        #Items table

        #Inserting data into Items table
        print("Inserting data into Items table")
        sql_command_insert_data_into_table = """INSERT INTO Items (Location_name, Price , Drink_type , Drink_flavour, Drink_size) VALUES (%s, %s, %s,%s,%s) """
        cursor.executemany(sql_command_insert_data_into_table, convert_none_data_to_null(unique_items))
        connection.commit()
        cursor.close()


def insert_data_into_orders_table(data, connection):
    print("insert_data_into_orders_tables")
    """Inserts data into orders table"""

    with connection.cursor() as cursor:
        print("got cursor")
        
        #Tier 3

        #Reformatted name/times/purchases info 
        customer_ids_list = data["Customer_id"]
        datetimes =  data["datetime"]
        all_purchases = reformat_purchases_info_for_sql(data, "ALL_PURCHASES")

        print("Grabbing customer_ids/times/purchases info")

        #Orders table

        #Looping through datetimes, which have 1-to-1 relationship with customer_ids/payment_ids
        #Selecting Time_id and appending to time_ids list
        print("Selecting Time_id and appending to time_ids list")
        time_ids = []
        for time in datetimes:
            cursor.execute("""SELECT t.Time_id From Time AS t WHERE t.datetime = %s""", (time, ))
            connection.commit()
            time_id = cursor.fetchone()[0]
            time_ids.append(time_id)

        #Looping through customer_ids list
        #Selecting payment_id connected to a customer_id
        print("Selecting payment_id connected to a customer_id which is connected to customer name")
        payment_ids = []
        for customer_id in customer_ids_list:
            cursor.execute("""SELECT p.Payment_id FROM Payments AS p WHERE p.Customer_id = %s; """, (customer_id, ))
            connection.commit()
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
                    connection.commit()
                    
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
                    connection.commit()
                    
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
                    connection.commit()
                    
                    time_id = cursor.fetchone()[0]
                    payment_id = purchase[0] 
                    time_id = purchase[2]
                    
                    orders_info.append((payment_id, item_id,time_id))
                
                else:
                    cursor.execute("""SELECT i.Item_id FROM  
                    Items AS i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour = %s
                    AND Drink_size= %s """, drink_order)
                    connection.commit()
                    
                    item_id = cursor.fetchone()[0]
                    payment_id = purchase[0]
                    time_id = purchase[2]
                    
                    orders_info.append((payment_id,item_id,time_id))
        
        print("executing many")
        sql_command_insert_data_into_table = """INSERT INTO Orders (Payment_id, Item_id, Time_id) VALUES (%s, %s, %s)"""           
        cursor.executemany(sql_command_insert_data_into_table, orders_info)
        connection.commit()
        cursor.close()


def insert_data_into_all_tables(data, connection):
    print("insert_data_into_all_tables")
    """Inserts data into various database tables"""
    try:
        insert_data_into_customer_table(data, connection)
        insert_data_cafe_locations_table(data, connection)
        insert_data_into_day_month_year_tables(data, connection)
        insert_data_into_payments_table(data, connection)
        insert_data_into_full_datetime_table(data, connection)
        insert_data_into_items_table(data,connection)
        insert_data_into_orders_table(data,connection)
    finally:
        connection.close()