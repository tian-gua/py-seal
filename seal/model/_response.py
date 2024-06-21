from typing import TypeVar
from pydantic import BaseModel

T = TypeVar('T')


class Response(BaseModel):
    code: int = 0
    message: str = ''
    data: T | None = None

    @staticmethod
    def build(data: T | None = None):
        response = Response()
        response.data = data
        return response

    def success(self, message='success'):
        self.code: int = 0
        self.message = message
        return self

    def error(self, code: int = -1, message='error'):
        self.code = code
        self.message = message
        return self
