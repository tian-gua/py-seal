import threading
import asyncio
from ._ws_dispatcher import dispatcher
from websockets.server import serve
from .. import seal
from loguru import logger


async def dispatch(websocket):
    path = websocket.path
    async for message in websocket:
        await dispatcher.dispatch(path)(message, websocket)


async def start_server():
    async with serve(dispatch, seal.get_config('seal', 'ws', 'host'), seal.get_config('seal', 'ws', 'port')):
        logger.info(f'启动 websocket 服务, 监听 {seal.get_config("seal", "ws", "host")}:{seal.get_config("seal", "ws", "port")}')
        await asyncio.get_running_loop().create_future()  # run forever


def start_background():
    threading.Thread(target=asyncio.run, args=(start_server(),)).start()
