import uuid
from contextvars import ContextVar

from loguru import logger

from seal.db.protocol import IDataSource
from seal.db.protocol import IDatabaseConnection


class SqlContext:
    def __init__(self):
        self._tx: IDatabaseConnection | None = None
        self._tx_id = None

    def begin(self, ds: IDataSource):
        self._tx_id = uuid.uuid4()
        self._tx = ds.get_connection()
        self._tx.begin()

        logger.debug(f'begin transaction: {self._tx_id}')

    def commit(self):
        self._tx.commit()
        logger.debug(f'commit transaction: {self._tx_id}')
        self._tx.close()

    def rollback(self):
        self._tx.rollback()
        logger.debug(f'rollback transaction: {self._tx_id}')
        self._tx.close()

    def tx(self):
        return self._tx

    def tx_id(self):
        return self._tx_id


sql_context = ContextVar('sql_context', default=SqlContext())
