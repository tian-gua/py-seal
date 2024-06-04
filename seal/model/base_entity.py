from datetime import datetime

from pydantic import BaseModel


class BaseEntity(BaseModel):
    id: int | None = None
    create_by: int | None = None
    update_by: int | None = None
    create_at: datetime | None = None
    update_at: datetime | None = None
    deleted: int | None = 0
