import etl.load.mysql_database_connection as db_connect
sql_code_filepath = './etl/load/database_sql_code.txt'

def create_database_tables(filepath):
    sql_db = db_connect.SQL_database()
    connection = sql_db.make_connection()

    try:
        with open(filepath, "r") as file_data:
            sql_code = " ".join(file_data.readlines())
            #NOTE: this method does not work for delimiters within sql statements e.g. Functions, Procedure
            commands = sql_code.split(';\n')
        with connection.cursor() as cursor:
            for command in commands:
                cursor.execute(command)
    except:
        print("Error with mysql connection and sql commands.")
    finally:
        connection.close()


if __name__ == "__main__":
    create_database_tables(sql_code_filepath)



