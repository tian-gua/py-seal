import jwt
from datetime import datetime, timedelta
from .. import get_config


def generate_token(**payloads):
    payloads["exp"] = datetime.now() + timedelta(seconds=get_config('seal', 'authorization', 'expire'))
    return jwt.encode(payloads, get_config("jwt_key"), algorithm="HS256")
