import sqlite3

from ...config import Configuration
from ..connector import DBConnector
from ...wrapper import singleton


@singleton
class SqliteConnector(DBConnector):
    def __init__(self):
        self.config = Configuration().config['mysql']

    def get_connection(self):
        return sqlite3.connect(self.config['database'])
