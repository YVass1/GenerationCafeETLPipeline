import pymysql
from pymysql.constants import CLIENT

class MySQL_Server:
    def __init__(self, host = "localhost",port = 33066,user = "root", password = "password", client_flag = CLIENT.MULTI_STATEMENTS, autocommit = True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.autocommit = autocommit
        self.client_flag = client_flag

    def make_connection(self):
        return pymysql.connect(
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.password,
            autocommit = self.autocommit,
            client_flag = self.client_flag
            )

class MYSQL_database:
    def __init__(self, database_name, host = "localhost",port = 33066,user = "root", password = "password", client_flag = CLIENT.MULTI_STATEMENTS, autocommit = True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = database_name
        self.autocommit = autocommit
        self.client_flag = client_flag

    def make_connection(self):
        return pymysql.connect(
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.password,
            db = self.db,
            autocommit = self.autocommit,
            client_flag = self.client_flag
            )

if __name__ == "__main__":
   pass
