import psycopg2
import sys
import os
import csv
import json
import boto3
import logging
import datetime 
from dotenv import load_dotenv


def start(event, context):
    print("Team One Pipeline")
    
    BUCKET_NAME = "cafe-data-data-pump-dev-team-1"
    SQL_TEXTFILE_KEY_NAME = "create_tables_postgresql.txt"

    # sql_code = read_from_s3(BUCKET_NAME, SQL_TEXTFILE_KEY_NAME)

    load_dotenv()
    conn = redshift_connect()
    logging.getLogger().setLevel(0)

    transformed_json = get_json_from_queue(event)
    transformed_dict = convert_json_to_dict(transformed_json)

    sql_code = SQL_TEXTFILE_KEY_NAME #temporary line to avoid error, sql code may be removed soon
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

# def read_from_s3(bucket, sql_txtfile_key):
#     s3 = boto3.client('s3')

#     s3_sql_code = s3.get_object(Bucket = bucket, Key = sql_txtfile_key)

#     sql_code = s3_sql_code['Body'].read().decode('utf-8')

#     return (sql_code)

def load(cleaned_data, connection, sql_code_txtfile):
    
    # create_database_tables(sql_code_txtfile, connection)

    insert_data_into_tables(cleaned_data, connection)

    return

################## LOAD SECTION ################

# def create_database_tables(sql_code_string, connection):
#     """Arguments: filepath, database name. Programmatically populates specified database
#      with tables using file containing SQL commands."""

#     try:
#         #Reformatting string to remove line breaks and tabs
#         #re is an imported module for formatting
#         reformatted_sql_string = re.sub(r"[\n\t]*", "", sql_code_string).strip()
        
#         #creating list of strings each contaning SQL statement
#         sql_string_list = reformatted_sql_string.split(";")
        
#         #Executing each SQL statement 
#         with connection.cursor() as cursor:
#             for sql_command in sql_string_list:
#                 if type(sql_command) == str:
#                     if sql_command.isspace() == False and len(sql_command) > 0:
#                         print(sql_command)
#                         cursor.execute(sql_command)
#                         connection.commit()
#             cursor.close()

#     except Exception as e:
#         print("Executing empty string")
#         print(f"Exception Error: {e}")


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
        unique_years_as_tuple_list.append((year, ))

    return days, unique_days_as_tuple_list, months, unique_months_as_tuple_list, years, unique_years_as_tuple_list


#Function to reformat data from dictionary for data to be suitable for MySQL statements 
def reformatting_data_for_sql(data):
    print("reformatting_data_for_sql")
    """Extracts from input dictionary and reformats relevant data into a required format for MySQL statements. """
    
    first_names  = data["fname"]
    last_names = data["lname"]
    customer_names = list(zip(first_names, last_names))

    locations = data["location"]
    location_set = list(set(locations))
    unique_locations = []

    for loc in location_set:
        unique_locations.append((loc, ))

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
            print(customer_names)
            #inserting data into customer table
            sql_command_insert_data_into_table = 'INSERT INTO Customers (Forename, Surname) VALUES (%s, %s)'
            cursor.executemany(sql_command_insert_data_into_table, customer_names)
            connection.commit()
            print("inserting data cafe location table")
            print(unique_locations)
            #inserting data into cafes locations table
            sql_command_insert_data_into_table = 'INSERT INTO Cafe_locations (Location_name) VALUES (%s)'
            cursor.executemany(sql_command_insert_data_into_table, unique_locations)
            connection.commit()
            print("inserting data day;month;year tables")
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

            #tier 2

            #inserting data into payments table
            #selecting corresponding customer ids
            print("selecting corresponding customer ids")
            cursor.execute('SELECT c.Customer_id FROM Customers AS c')
            connection.commit()

            #Extracts each row's customer_id but in tuple form: (customer_id, )
            print("Extracts each row's customer_id but in tuple form: (customer_id, )") 
            rows_of_customer_ids_tuples = cursor.fetchall()

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
            connection.commit()

            #Inserting data into table Time
            #First check datetimes corresponding day/month/year and then extract its id from tables 
            #Then id data insert into time table

            #Looping through days list which is 1-to-1 mapping with datetimes list
            #For datetimes corresponding day e.g. Monday, extracting its Day_id and
            #append to day_ids list
            print("append to day_ids list")
            day_ids = []
            for day in days:
                cursor.execute(f'SELECT d.Day_id FROM Day AS d WHERE d.Day = %s', (day, ) )
                connection.commit()
                day_id = cursor.fetchone()[0]
                day_ids.append(day_id)

            #Looping through months list which is 1-to-1 mapping with datetimes list
            #For datetimes corresponding month e.g. July, extracting its Month_id and
            #append to month_ids list
            print("append to month_ids list")
            month_ids = []
            for month in months:
                cursor.execute(f'SELECT d.Month_id FROM Month AS d WHERE d.Month = %s', (month, ) )
                connection.commit()
                month_id = cursor.fetchone()[0]
                month_ids.append(month_id)
            
            #Looping through years list which is 1-to-1 mapping with datetimes list
            #For datetimes corresponding day e.g. 2020, extracting its Year_id and
            #append to year_ids list
            print("append to year_ids list")
            year_ids = []
            for year in years:
                cursor.execute(f'SELECT d.Year_id FROM Year AS d WHERE d.Year = %s', (year, ) )
                connection.commit()
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
            sql_command_insert_data_into_table = """INSERT INTO Time (datetime,Day_id,Month_id,Year_id) VALUES (%s, %s,%s,%s)"""
            cursor.executemany(sql_command_insert_data_into_table, unique_datetimes)
            connection.commit()

            #Items table

            #Inserting data into Items table
            print("inserting data into Items table")
            sql_command_insert_data_into_table = """INSERT INTO Items (Location_name, Price , Drink_type , Drink_flavour, Drink_size) VALUES (%s, %s, %s,%s,%s)"""
            cursor.executemany(sql_command_insert_data_into_table, convert_none_data_to_null(unique_items))
            connection.commit()
            
            #tier 3

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

            #Looping through customer_names list
            #Selecting payment_id connected to a customer_id which is connected to customer name
            print("Selecting payment_id connected to a customer_id which is connected to customer name")
            payment_ids = []
            for name in customer_names:
                cursor.execute("""select p.Payment_id from Payments AS p join Customers AS c on c.Customer_id = p.Customer_id where c.forename = %s and c.surname = %s; """, name)
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
    # except Exception as e:
    #     #Rolls back any sql statements committed when error occurs partway to perserve data integrity
    #     connection.ROLLBACK()
    #     print(f"Exception Error: {e}")
    finally:
        connection.close()