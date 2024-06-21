import jwt
from datetime import datetime, timedelta
from .. import seal


def generate_token(**payloads):
    try:
        payloads["exp"] = datetime.now() + timedelta(seconds=seal.get_config('seal', 'authorization', 'expire'))
        token = jwt.encode(payloads, seal.get_config('seal', 'authorization', 'jwt_key'), algorithm="HS256")
    except Exception as e:
        raise Exception(f"Token generation failed: {e}")
    return token
