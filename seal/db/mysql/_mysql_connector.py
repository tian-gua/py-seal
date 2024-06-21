import pymysql
import pymysql.cursors
from ..connector import Connector
from ... import seal
from ...wrapper import singleton


@singleton
class MysqlConnector(Connector):
    def __init__(self):
        self.config = seal.get_config('seal', 'mysql')
        # self.pool = mysql.connector.pooling.MySQLConnectionPool(**self.config)

    def get_connection(self):
        # return self.pool.get_connection()
        return pymysql.connect(host=self.config['host'],
                               user=self.config['user'],
                               password=self.config['password'],
                               database=self.config['database'],
                               cursorclass=pymysql.cursors.DictCursor)
