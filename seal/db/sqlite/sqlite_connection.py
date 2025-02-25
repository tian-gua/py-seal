from sqlite3 import Connection


class SqliteConnection:

    def __init__(self, connection: Connection):
        self.connection = connection

    def close(self):
        self.connection.close()

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        self.connection.execute('COMMIT')

    def rollback(self):
        self.connection.execute('ROLLBACK')

    def begin(self):
        self.connection.execute("BEGIN TRANSACTION")

    def ping(self, seconds):
        pass
