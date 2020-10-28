import etl.load.mysql_database_connection as db_connect
import re
from datetime import datetime
import numpy as np
from etl.load.mock_etl_handler_local import start as start

data = start()

#CONSTANTS NEEDED
sql_code_filepath = './etl/load/database_sql_code.txt'

def create_database(database_name):
    mysql_server = db_connect.MySQL_Server()
    connection = mysql_server.make_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`;")
            connection.commit()
    except Exception as e:
        print(f"Exception Error: {e}")
    finally:
        connection.close()

def create_database_tables(filepath, database_name):
    global mysql_db
    mysql_db = db_connect.MYSQL_database(database_name)
    connection = mysql_db.make_connection()
    try:
        with open(filepath, "r") as file_data:
            sql_code = "".join(file_data.readlines())
            commands_string = re.sub(r"[\n\t]*", "", sql_code)
        with connection.cursor() as cursor:
            cursor.execute(commands_string)
    except Exception as e:
        print(f"Exception Error: {e}")
    finally:
        connection.close()

sub_data =  [{"drink_size": ["large", "medium"], "drink_type": ["tea", "coffee"], "drink_flavour": [None, "black"],
"drink_price": [2.5, 1.75]}, {"drink_size": [None, "medium"], "drink_type": ["latte", "coffee"], "drink_flavour": ["peppermint", "black"],
"drink_price": [10.5, 1.75]}]

# data = {"datetime": ["2020-10-11 08:11:00", "2020-10-26 11:06:00"], "location": 
# ["Aberdeen","Aberdeen"], "fname": ["John", "Maria"],"lname" : ["Doe", "Johnson"],
#  "purchase" : sub_data, "total_price" : [4.25, 2.10],"payment_method" : ["CASH","CARD"],
#   "card_number" : [None,"************1111"]}

def check_for_none_data(data):
    #if you see None in the dict replace it with null
    # list_of_tuples = [("tea","black"),("coffee", None, 2.0)]
    if isinstance(data,list):
        for tup in data:
            for element in tup:
                if element is None:
                    element = "NULL"
                elif isinstance(element, float):
                    pass
    return data

def check_for_none_data1(data):
    for outer_list in data:
        for inner_list in outer_list:
            for element in inner_list:
                if isinstance(element, float):
                    pass
                elif element is None:
                    element = "NULL"
    return data

def null_to_none(element):
    if element == "Null":
        element = None
    return element
                 

def drink_flavour_is_none(tuple_data):
    my_list = []
    is_none = False
    if tuple_data[3] is None:
        is_none = True
    return is_none
def drink_size_is_none(tuple_data):
    my_list = []
    is_none = False
    if tuple_data[4] is None:
        is_none = True
    return is_none


# def check_tuple(data):
#     my_list = []
#     for element in data:
#         my_list.append(element)
#         if isinstance(element, float):
#             continue
#         elif element is None:
#             element = "NULL"
        
#     return data

        
def reformatting_data_for_sql(data):
    #extracting data from dict format to format more suitable for MySQL statements
        first_names  = data["fname"]
        last_names = data["lname"]
        customer_names = list(zip(first_names, last_names))

        locations = data["location"]
        unique_locations = list(set(locations))

        datetimes = data["datetime"]
        datetime_objects = [datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S') for dtime in datetimes]
        days = [dtime.strftime("%A") for dtime in datetime_objects]
        unique_days = list(set(days))
        months = [dtime.strftime("%B") for dtime in datetime_objects]
        unique_months = list(set(months))
        years = [dtime.strftime("%Y") for dtime in datetime_objects]
        unique_years = list(set(years))

        total_prices = data["total_price"]
        payment_methods = data["payment_method"]
        card_numbers = data["card_number"]
        
        #purchases may still need restructuring

        all_purchases = [list(zip(data["location"],purchase["drink_price"], purchase["drink_type"], purchase["drink_flavour"],purchase["drink_size"])) for purchase in data["purchase"]] 
        x = [tup for list_of_tup in all_purchases for tup in list_of_tup]
        unique_items = list(set(x))

        return datetimes, customer_names, unique_locations, days, unique_days, months, unique_months, years,unique_years, total_prices, payment_methods, card_numbers, unique_items, all_purchases, x

def insert_data_into_tables(data):
    connection = mysql_db.make_connection()
    try:
        #making connection to database
        with connection.cursor() as cursor:

            #reformatting data for sql
            datetimes, customer_names, unique_locations, days, unique_days, months, unique_months, years, unique_years, total_prices, payment_methods, card_numbers, unique_items, all_purchases, x = reformatting_data_for_sql(data)

            #insert data into tables in correct order due to dependencies (tier1 --> tier2 --> tier3)

            #tier 1

            #inserting data into customer table
            sql_command_insert_data_into_table = 'INSERT INTO `Customers` (`Forename`, `Surname`) VALUES (%s, %s)'
            cursor.executemany(sql_command_insert_data_into_table, customer_names)

            #inserting data into cafes locations table
            sql_command_insert_data_into_table = 'INSERT INTO `Cafe_locations` (`Location_name`) VALUES (%s)'
            cursor.executemany(sql_command_insert_data_into_table, unique_locations)

            #inserting data into day, month, year tables

            sql_command_insert_data_into_table = "INSERT INTO `Day` (`Day`) VALUES (%s);"
            cursor.executemany( sql_command_insert_data_into_table, unique_days)
            sql_command_insert_data_into_table = "INSERT INTO `Month` (`Month`) VALUES (%s);"
            cursor.executemany( sql_command_insert_data_into_table, unique_months)
            sql_command_insert_data_into_table = "INSERT INTO `Year` (`Year`) VALUES (%s);"
            cursor.executemany( sql_command_insert_data_into_table, unique_years)

            #tier 2

            #inserting data into payments table
            #selecting corresponding customer ids
            cursor.execute('SELECT c.Customer_id FROM Customers as c;')
            customer_ids = [row[0] for row in cursor.fetchall()]

            #reformatting data --> each customer_id with customer's payment info in a tuple
            payments_info = list(zip(customer_ids, total_prices, payment_methods, card_numbers))
            
            #inserting payment info into payments table
            sql_command_insert_data_into_table = """INSERT INTO `Payments` (`Customer_id`,`Total_amount`,`Payment_type`,`Card_number`) VALUES (%s, %s, %s,%s) ;"""
            cursor.executemany(sql_command_insert_data_into_table, check_for_none_data(payments_info))

            #Time table
            #First need to check datetimes corresponding day/month/year and then grab its id from tables #Then id data insert into time table

            day_ids = []
            for day in days:
                cursor.execute(f'SELECT d.Day_id FROM Day as d WHERE d.Day = %s', day )
                day_ids.append(cursor.fetchone()[0])

            month_ids = []
            for month in months:
                cursor.execute(f'SELECT d.Month_id FROM Month as d WHERE d.Month = %s', month )
                month_ids.append(cursor.fetchone()[0])
            
            year_ids = []
            for year in years:
                cursor.execute(f'SELECT d.Year_id FROM Year as d WHERE d.Year = %s', year )
                year_ids.append(cursor.fetchone()[0])
        
            full_datetime_info = list(zip(datetimes, day_ids, month_ids, year_ids))

            unique_datetimes =  list(set(full_datetime_info))
            sql_command_insert_data_into_table = """INSERT INTO `Time` (`datetime`,`Day_id`,`Month_id`,`Year_id`) VALUES (STR_TO_DATE(%s, "%%Y-%%m-%%d %%H:%%i:%%S"), %s,%s,%s);"""
            cursor.executemany(sql_command_insert_data_into_table, unique_datetimes)

            #Items table

            sql_command_insert_data_into_table = """INSERT INTO `Items` (`Location_name`,`Price`,`Drink_type`,`Drink_flavour`, `Drink_size`) VALUES (%s, %s, %s,%s,%s) ;"""
            cursor.executemany(sql_command_insert_data_into_table, check_for_none_data(unique_items))
            
            #tier 3
            #Orders table


            #select time_ids
            time_ids = []
            for time in datetimes:
               cursor.execute("""SELECT t.Time_id From Time as t WHERE t.datetime = %s""", time)
               time_ids.append(cursor.fetchone()[0])

            payment_ids = []
            for name in customer_names:
                cursor.execute("""select p.Payment_id from Payments as p join Customers as c on c.Customer_id = p.Customer_id where c.forename = %s and c.surname = %s; """, name)
                payment_ids.append(cursor.fetchone()[0])

            orders = list(zip(payment_ids, check_for_none_data1(all_purchases), time_ids))
            
            #select items ids 
            item_ids = []
            orders_info = []
            for order in orders:
                for drink_order in order[1]:
                    if drink_flavour_is_none(drink_order) and drink_size_is_none(drink_order):
                        drink_order_list = list(drink_order)
                        del drink_order_list[3:]
                        cursor.execute("""SELECT i.Item_id FROM  
                        Items as i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour IS NULL
                        AND Drink_size IS NULL """, drink_order_list)
                        a = cursor.fetchone()
                        orders_info.append((order[0],a[0], order[2]))
                    elif drink_flavour_is_none(drink_order):
                        drink_order_list =  list(drink_order)
                        drink_order_list.remove(drink_order_list[3])
                        cursor.execute("""SELECT i.Item_id FROM  
                        Items as i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour IS NULL
                        AND Drink_size = %s """, drink_order_list)
                        a = cursor.fetchone()
                        orders_info.append((order[0],a[0], order[2]))
                    elif drink_size_is_none(drink_order):
                        drink_order_list = list(drink_order)
                        drink_order_list.remove(drink_order_list[4])
                        cursor.execute("""SELECT i.Item_id FROM  
                        Items as i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour = %s
                        AND Drink_size IS NULL """, drink_order_list)
                        a = cursor.fetchone()
                        orders_info.append((order[0],a[0], order[2]))
                    else:
                        cursor.execute("""SELECT i.Item_id FROM  
                        Items as i WHERE i.Location_name = %s AND i.Price = %s AND i.Drink_type = %s AND i.Drink_flavour = %s
                        AND Drink_size= %s """, drink_order)
                        a = cursor.fetchone()
                        orders_info.append((order[0],a[0], order[2]))

                    
            
            sql_command_insert_data_into_table = """INSERT INTO `Orders` (Payment_id, Item_id, Time_id)  VALUES (%s, %s, %s) ;"""           
            cursor.executemany(sql_command_insert_data_into_table, orders_info)

    # except:
    #     #connection.rollback() so when errors occurs integrity of data perserved?
    #     print("Exception Error")
    finally:
        connection.close()

if __name__ == "__main__":
    create_database("final_project_team1")
    create_database_tables(sql_code_filepath, "final_project_team1")
    insert_data_into_tables(data)
