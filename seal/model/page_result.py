from pydantic import BaseModel


class PageResult(BaseModel):
    page: int
    page_size: int
    total: int
    data: list | None
