import time
from threading import Lock

import pymysql
from loguru import logger
from pymysql.connections import Connection as PyMySQLConnection
from pymysql.cursors import DictCursor

from seal.enum.connection_status import ConnectionStatus


class MysqlConnection:
    def __init__(self, connection: PyMySQLConnection, pool: 'ConnectionPool', create_time=time.time()):
        self.connection: PyMySQLConnection = connection
        self.pool: ConnectionPool = pool
        self.status: ConnectionStatus = ConnectionStatus.IDLE
        self.create_time = create_time
        self.conn_lock = Lock()

    def close(self):
        self.pool.release(self)
        self.conn_lock.release()

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def begin(self):
        self.conn_lock.acquire_lock()
        self.connection.begin()

    def ping(self):
        try:
            if not self.conn_lock.acquire_lock(timeout=10):
                raise Exception(f'Connection [{id(self)}] is locked')
            self.connection.ping(reconnect=True)
        finally:
            self.conn_lock.release()

    def insert_id(self):
        return self.connection.insert_id()


class ConnectionPool:
    def __init__(self, conf):
        self.conf = conf

        self._connections: list[MysqlConnection] = []
        self._min_connections = conf['pool']['min_connections']
        self._max_connections = conf['pool']['max_connections']

        for _ in range(self._min_connections):
            mysql_connection: PyMySQLConnection = pymysql.connect(host=conf['host'],
                                                                  port=conf['port'],
                                                                  user=conf['user'],
                                                                  password=conf['password'].encode('utf-8'),
                                                                  database=conf['database'],
                                                                  cursorclass=DictCursor)
            self._connections.append(MysqlConnection(mysql_connection, self))

    def new_connection(self):
        return pymysql.connect(host=self.conf['host'],
                               port=self.conf['port'],
                               user=self.conf['user'],
                               password=self.conf['password'].encode('utf-8'),
                               database=self.conf['database'],
                               cursorclass=DictCursor)

    def get_connection(self, timeout=3):
        start_time = time.time()

        connection = self.occupy()
        if connection is not None:
            return connection

        if len(self._connections) < self._max_connections:
            mysql_connection = self.new_connection()
            connection = MysqlConnection(mysql_connection, self)
            connection.status = ConnectionStatus.OCCUPIED
            self._connections.append(connection)
            return connection

        if timeout is not None:
            raise Exception('No available connection')

        while True:
            connection = self.occupy()
            if connection is not None:
                return connection
            else:
                time.sleep(0.1)

                if timeout is not None and time.time() - start_time > timeout:
                    raise Exception('No available connection')

    def occupy(self) -> MysqlConnection | None:
        for connection in self._connections:
            if connection.status == ConnectionStatus.IDLE:
                connection.status = ConnectionStatus.OCCUPIED
                return connection
        return None

    def release(self, mysql_connection: MysqlConnection):
        if len(self._connections) > self._min_connections:
            mysql_connection.connection.close()
            self._connections.remove(mysql_connection)
        else:
            mysql_connection.status = ConnectionStatus.IDLE

    def ping(self, seconds=60):
        while True:
            time.sleep(seconds)
            ids = []
            for connection in self._connections:
                ids.append(id(connection))
                connection.ping()
            logger.info(f'active connections {ids}')
