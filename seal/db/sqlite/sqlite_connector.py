import sqlite3

from db.connector import DBConnector
from wrapper.decorator import singleton


@singleton
class SqliteConnector(DBConnector):
    def __init__(self):
        self.connection = sqlite3.connect('./db.sqlite')

    def get_connection(self):
        return self.connection
