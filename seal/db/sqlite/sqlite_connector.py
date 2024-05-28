import sqlite3

from seal.config.configuration import Configuration
from seal.db.connector import DBConnector
from seal.wrapper.decorator import singleton


@singleton
class SqliteConnector(DBConnector):
    def __init__(self):
        self.config = Configuration().config['mysql']

    def get_connection(self):
        return sqlite3.connect(self.config['database'])
