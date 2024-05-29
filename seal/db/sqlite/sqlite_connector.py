import sqlite3

from ... import get_seal
from ..connector import DBConnector
from ...wrapper import singleton


@singleton
class SqliteConnector(DBConnector):
    def __init__(self):
        self.config = get_seal().get_config('sqlite')

    def get_connection(self):
        return sqlite3.connect(self.config['path'])
