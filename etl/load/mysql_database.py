import etl.load.mysql_database_connection as db_connect
import re
from datetime import datetime

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


sub_data =  [{"drink_size": ["large", "medium"], "drink_type": ["tea", "coffee"], "drink_flavour": ["peppermint", "black"],
 "drink_price": [2.5, 1.75]}, {"drink_size": ["small", "medium"], "drink_type": ["latte", "coffee"], "drink_flavour": ["peppermint", "black"],
 "drink_price": [10.5, 1.75] }]

 data = {"datetime": ["11/10/2020 08:11", "26/10/2020 11:06"], "location": ["Aberdeen","Aberdeen"], "fname": ["John", "Maria"],"lname" : ["Doe", "Johnson"], "purchase" : sub_data, "total_price" : [4.25, 2.10],"payment_method" : ["CARD","CARD"], "card_number" : ["************1234","************1111"]}


def reformatting_data_for_sql(data):
    #extracting data from dict format to format more suitable for MySQL statements
        first_names  = data["fname"]
        last_names = data["lname"]
        customer_names = list(zip(first_names, last_names))

        locations = data["location"]
        unique_locations = list(set(locations))

        datetimes = data["datetime"]
        datetime_objects = [datetime.strptime(dtime, '%d/%m/%Y %H:%M') for dtime in datetimes]
        days = [dtime.strftime("%A") for dtime in datetime_objects]
        unique_days = list(set(days))
        months = [dtime.strftime("%B") for dtime in datetime_objects]
        unique_months = list(set(months))
        years = [dtime.strftime("%Y") for dtime in datetime_objects]
        unique_years = list(set(years))

        total_prices = data["total_price"]
        payment_methods = data["payment_method"]
        card_numbers = data["card_number"]
        
        #purchases still need restructure
        purchases_dict = data["purchase"] #list of dictionary

        prices = [purchase["drink_price"] for purchase in data["purchase"]]
        drink_types = [purchase["drink_type"] for purchase in data["purchase"]]
        print(drink_types)
        drink_flavours = [purchase["drink_flavour"] for purchase in data["purchase"]]
        drink_sizes = [purchase["drink_size"] for purchase in data["purchase"]]

        return datetimes, customer_names, unique_locations, days, unique_days, months, unique_months, years,unique_years, total_prices, payment_methods, card_numbers

def insert_data_into_tables(data):
    connection = mysql_db.make_connection()
    try:
        #making connection to database
        with connection.cursor() as cursor:

            #reformatting data for sql
            datetimes, customer_names, unique_locations, days, unique_days, months, unique_months, years, unique_years, total_prices, payment_methods, card_numbers = reformatting_data_for_sql(data)

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
            cursor.executemany(sql_command_insert_data_into_table, payments_info)

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

            sql_command_insert_data_into_table = """INSERT INTO `Time` (`datetime`,`Day_id`,`Month_id`,`Year_id`) VALUES (STR_TO_DATE(%s, "%%d/%%m/%%Y %%H:%%i"), %s,%s,%s);"""
            cursor.executemany(sql_command_insert_data_into_table, full_datetime_info)

            #Items table

            #grabbing cafe location name where drink type ordered location matches
            #cursor.execute('SELECT cafe.Location_name FROM Cafe_Locations as cafe WHERE cafe.Location_name = %s; locations')
            #location_names = [row[0] for row in cursor.fetchall()]


            items_info = [list(zip(location_names, prices, drink_types, drink_flavours, drink_sizes))]
            unique_items = list(set(items_info))
            sql_command_insert_data_into_table = """INSERT INTO `Items` (`Location_name`,`Price`,`Drink_type`,`Drink_flavour`, `Drink_size`) VALUES (%s, %s, %s,%s,%s) ;"""
            cursor.executemany(sql_command_insert_data_into_table, unique_items)
            
            #tier 3
            #Orders table

    except Exception as e:
        #connection.rollback() so when errors occurs integrity of data perserved?
        print(f"Exception Error: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    create_database("final_project_team1")
    create_database_tables(sql_code_filepath, "final_project_team1")
    insert_data_into_tables(data)
