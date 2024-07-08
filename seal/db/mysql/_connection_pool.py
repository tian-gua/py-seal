import time
from ... import seal

from ._mysql_connector import MysqlConnector


class ConnectionPool:
    def __init__(self):
        self._connections: list[DelegateConnection] = []
        self._min_connections = seal.get_config('seal', 'mysql', 'pool', 'min_connections')
        self._max_connections = seal.get_config('seal', 'mysql', 'pool', 'max_connections')

        for _ in range(self._min_connections):
            mysql_connection = MysqlConnector().get_connection()
            self._connections.append(DelegateConnection(mysql_connection, self))

    def get_connection(self, timeout=None):
        start_time = time.time()

        connection = self._occupy()
        if connection is not None:
            return connection

        if len(self._connections) < self._max_connections:
            mysql_connection = MysqlConnector().get_connection()
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

            connection = self._occupy()
            if connection is not None:
                return connection

    def _occupy(self):
        for connection in self._connections:
            if connection.status == 'idle':
                connection.occupy()
                return connection
        return None

    def release(self, connection):
        if len(self._connections) > self._min_connections:
            connection.connection.close()
            self._connections.remove(connection)
        else:
            connection.status = 'idle'


class DelegateConnection:
    def __init__(self, connection, pool, create_time=time.time()):
        self._connection = connection
        self.pool = pool
        self.status = 'idle'
        self.create_time = create_time

    def close(self):
        self.pool.release(self._connection)

    def cursor(self):
        return self._connection.cursor()

    def occupy(self):
        self.status = 'occupied'
