import pymysql
import pymysql.cursors
from seal.config import config
from seal.db.connector import DBConnector
from seal.wrapper.decorator import singleton


@singleton
class MysqlConnector(DBConnector):
    def __init__(self):
        self.config = config['mysql']
        # self.pool = mysql.connector.pooling.MySQLConnectionPool(**self.config)

    def get_connection(self):
        # return self.pool.get_connection()
        return pymysql.connect(host=self.config['host'],
                               user=self.config['user'],
                               password=self.config['password'],
                               database=self.config['database'],
                               cursorclass=pymysql.cursors.DictCursor)
