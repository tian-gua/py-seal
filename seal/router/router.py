import asyncio
import re
import time
from functools import wraps

import jwt
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from loguru import logger
from starlette.middleware.cors import CORSMiddleware

from ..config import configurator
from ..context import web_context
from ..exception import BusinessException
from ..model import Response as ResponseModel


async def verify_token(request: Request = Request):
    if request.method == 'OPTIONS':
        return None

    ignore = False
    path = request.url.path
    urls = configurator.get_config('seal', 'authorization', 'excludes')
    for url in urls:
        # * -> [a-zA-Z0-9_\-]*, ** -> .*
        url = url.replace('*', '[a-zA-Z0-9_\-]*').replace('**', '.*')
        if re.match(url, path):
            ignore = True
            break

    try:
        token = request.headers.get('Authorization', None)
        if token is None or token == '':
            if ignore:
                return None
            raise HTTPException(status_code=401, detail="Token is required")

        payload = jwt.decode(token,
                             configurator.get_config('seal', 'authorization', 'jwt_key'),
                             algorithms=["HS256"])
        web_context.get().set_uid(payload['uid'])
    except Exception as e:
        if ignore:
            return None
        raise HTTPException(status_code=401, detail=f"Token is invalid: {e}")


app = FastAPI(dependencies=[Depends(verify_token)])


@app.middleware("http")
async def calc_time(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(f'{request.method} {request.url.path} {response.status_code} {process_time}')
    response.headers["X-Process-Time"] = str(process_time)
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return Response(status_code=exc.status_code, content=exc.detail)


def response_body(func, **kwargs):
    @wraps(func)
    async def wrapper(*fun_args, **fun_kwargs):
        try:
            task = asyncio.create_task(func(*fun_args, **fun_kwargs))
            result = await task
            if 'response_model' in kwargs and type(kwargs.get('response_model')) == type(ResponseModel):
                return ResponseModel.build(result).success()
            return result
        except BusinessException as e:
            logger.exception(e)
            return ResponseModel.build().error(message=e.message, code=e.code)
        except Exception as e:
            logger.exception(e)
            return ResponseModel.build().error(message=str(e))

    return wrapper


def get(path: str, **kwargs):
    def decorator(func):
        if 'response_model' not in kwargs and 'response_class' not in kwargs:
            kwargs['response_model'] = ResponseModel
        return app.get(path, **kwargs)(response_body(func, **kwargs))

    return decorator


def post(path: str, **kwargs):
    def decorator(func):
        if 'response_model' not in kwargs and 'response_class' not in kwargs:
            kwargs['response_model'] = ResponseModel
        return app.post(path, **kwargs)(response_body(func, **kwargs))

    return decorator


def delete(path: str, **kwargs):
    def decorator(func):
        if 'response_model' not in kwargs and 'response_class' not in kwargs:
            kwargs['response_model'] = ResponseModel
        return app.delete(path, **kwargs)(response_body(func, **kwargs))

    return decorator


def put(path: str, **kwargs):
    def decorator(func):
        if 'response_model' not in kwargs and 'response_class' not in kwargs:
            kwargs['response_model'] = ResponseModel
        return app.put(path, **kwargs)(response_body(func, **kwargs))

    return decorator
