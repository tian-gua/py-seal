import sqlite3

from ... import seal
from ..connector import Connector
from ...wrapper import singleton


@singleton
class SqliteConnector(Connector):
    def __init__(self):
        self.config = seal.get_config('seal', 'sqlite')

    def get_connection(self):
        return sqlite3.connect(self.config['path'])
