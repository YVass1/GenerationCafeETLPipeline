import etl.load.mysql_database_connection as db_connect
import re

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

data = {"datetime": ["11/10/2020 08:11"], "location": ["Aberdeen", "Aberdeen", "Aberdeen"], "fname": ["John", "Maria", "Jack"], "lname": ["Doe", "Johnson", "Bobby"],
 "purchase": [dict], "total_price": [4.25, 2.10, 3.60], "payment_method": ["CARD","CARD", "CASH"], "card_number": ["************1234","************1111" , '']}

sub_data =  {"drink_size": ["large", "medium"], "drink_type": ["tea", "coffee"], "drink_flavour": ["peppermint", "black"],
 "drink_price": [2.5, 1.75]}

def insert_data_into_tables(data):
    connection = mysql_db.make_connection()
    try:
        #extracting data from dict format to format more suitable for MySQL statements
        first_names  = data["fname"]
        last_names = data["lname"]
        customer_names = list(zip(first_names, last_names))

        locations = data["location"]
        unique_locations = list(set(locations))

        datetimes = data["datetime"]

        total_prices = data["total_price"]
        payment_methods = data["payment_method"]
        card_numbers = data["card_number"]
        payments = list(zip(total_prices, payment_methods, card_numbers))
        
        #purchases still need restructure
        purchases = data["purchase"] #list of dictionary

        #making connection to database
        with connection.cursor() as cursor:
            #insert data into tables in correct order due to dependencies (tier1 --> tier2 --> tier3)

            #tier 1

            #inserting data into customer table
            sql_command_insert_data_into_table = 'INSERT INTO `Customers` (`Forename`, `Surname`) VALUES (%s, %s)'
            cursor.executemany(sql_command_insert_data_into_table, customer_names)

            #inserting data into cafes locations table
            sql_command_insert_data_into_table = 'INSERT INTO `Cafe_locations` (`Location_name`) VALUES (%s)'
            cursor.executemany(sql_command_insert_data_into_table, unique_locations)

            #tier 2

            #inserting data into payments table
            #selecting corresponding customer ids
            cursor.execute('SELECT c.Customer_id FROM Customers as c;')
            customer_ids = [row[0] for row in cursor.fetchall()]

            #reformatting data --> each customer_id with customer's payment info in a tuple
            payments_info = list(zip(customer_ids, total_prices, payment_methods, card_numbers))
            
            #inserting payment info into payments table
            sql_command_insert_data_into_table = """INSERT INTO `Payments` (`Customer_id`,`Total_amount`,`Payment_type`,`Card_number`) VALUES (%s, %s, %s,%s) ;"""
            cursor.executemany( sql_command_data_insert_into_table, payments_info)

            #inserting data into time table
            sql_command_insert_data_into_table = 'INSERT INTO `Time` (`datetime`) VALUES (%s)'
            cursor.executemany(sql_command_insert_data_into_table, datetimes)

            #Items table

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
