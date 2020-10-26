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

data = {"datetime": ["11/10/2020 08:11"], "location": ["Aberdeen"], "fname": ["John"], "lname": ["Doe"],
 "purchase": [dict], "total_price": [4.25], "payment_method": ["CARD"], "card_number": ["************1234"]}

sub_data =  {"drink_size": ["large", "medium"], "drink_type": ["tea", "coffee"], "drink_flavour": ["peppermint", "black"],
 "drink_price": [2.5, 1.75]}

def insert_data_into_tables(data):
    connection = mysql_db.make_connection()
    try:
        with connection.cursor() as cursor:
                #make connection

                #tier 1
                first_names  = data["fname"]
                last_names = data["lname"]
                #insert into table customer values
                locations = data["location"]
                #insert into cafes table values locations
                cursor.executemany(operation)

                #tier 2

                #tier 3
                

    except Exception as e:
        #connection.rollback()
        print(f"Exception Error: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    create_database("final_project_team1")
    create_database_tables(sql_code_filepath, "final_project_team1")
