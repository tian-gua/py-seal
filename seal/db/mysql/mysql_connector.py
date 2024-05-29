import pymysql
import pymysql.cursors
from ..connector import DBConnector
from ...wrapper import singleton
from ... import get_seal


@singleton
class MysqlConnector(DBConnector):
    def __init__(self):
        self.config = get_seal().get_config('mysql')
        # self.pool = mysql.connector.pooling.MySQLConnectionPool(**self.config)

    def get_connection(self):
        # return self.pool.get_connection()
        return pymysql.connect(host=self.config['host'],
                               user=self.config['user'],
                               password=self.config['password'],
                               database=self.config['database'],
                               cursorclass=pymysql.cursors.DictCursor)
