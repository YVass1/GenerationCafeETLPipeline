import etl.load.mysql_database_connection as db_connect
import re
sql_code_filepath = './etl/load/database_sql_code.txt'

def create_database_tables(filepath):
    sql_db = db_connect.SQL_database()
    connection = sql_db.make_connection()
    try:
        with open(filepath, "r") as file_data:
            sql_code = "".join(file_data.readlines())
            operation = re.sub(r"[\n\t]*", "", sql_code)
        with connection.cursor() as cursor:
            cursor.execute(operation)
    except Exception as e:
        print(f"Exception Error: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    create_database_tables(sql_code_filepath)
