import time
import pymysql
from pymysql.cursors import DictCursor
from pymysql.connections import Connection as PyMySQLConnection


class ConnectionPool:
    def __init__(self, conf):
        self.conf = conf

        self._connections: list[DelegateConnection] = []
        self._min_connections = conf['pool']['min_connections']
        self._max_connections = conf['pool']['max_connections']

        for _ in range(self._min_connections):
            mysql_connection: PyMySQLConnection = pymysql.connect(host=conf['host'],
                                                                  user=conf['user'],
                                                                  password=conf['password'],
                                                                  database=conf['database'],
                                                                  cursorclass=DictCursor)
            self._connections.append(DelegateConnection(mysql_connection, self))

    def new_connection(self):
        return pymysql.connect(host=self.conf['host'],
                               user=self.conf['user'],
                               password=self.conf['password'],
                               database=self.conf['database'],
                               cursorclass=DictCursor)

    def get_connection(self, timeout=None):
        start_time = time.time()

        connection = self.occupy()
        if connection is not None:
            return connection

        if len(self._connections) < self._max_connections:
            mysql_connection = self.new_connection()
            connection = DelegateConnection(mysql_connection, self)
            connection.occupy()
            self._connections.append(connection)
            return connection

        if timeout is not None:
            raise Exception('No available connection')

        while True:
            time.sleep(0.1)

            if timeout is not None and time.time() - start_time > timeout:
                raise Exception('No available connection')

            connection = self.occupy()
            if connection is not None:
                return connection

    def occupy(self):
        for connection in self._connections:
            if connection.status == 'idle':
                connection.occupy()
                return connection
        return None

    def release(self, delegate_connection):
        if len(self._connections) > self._min_connections:
            delegate_connection.connection.close()
            self._connections.remove(delegate_connection)
        else:
            delegate_connection.status = 'idle'


class DelegateConnection:
    def __init__(self, connection: PyMySQLConnection, pool: ConnectionPool, create_time=time.time()):
        self.connection: PyMySQLConnection = connection
        self.pool: ConnectionPool = pool
        self.status = 'idle'
        self.create_time = create_time

    def close(self):
        self.pool.release(self)

    def cursor(self):
        return self.connection.cursor()

    def occupy(self):
        self.status = 'occupied'

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def begin(self):
        self.connection.ping(reconnect=True)
        self.connection.begin()
