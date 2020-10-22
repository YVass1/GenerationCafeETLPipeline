import pymysql

class SQL_database:
    def __init__(self, host = "localhost",port = 33066,user = "root", password = "password",db = "final_project-team1", autocommit = True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.autocommit = autocommit

    def make_connection(self):
        return pymysql.connect(
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.password,
            db = self.db,
            autocommit = self.autocommit )


if __name__ == "__main__":
   pass
