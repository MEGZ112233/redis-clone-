import sys
import asyncio

import app
from . import Parser
from . import commandHandler
from .expire import active_deleting
from app import expires, hash_table
from app.DBReader import DBReader
import os
from dataclasses import dataclass
from typing import Optional


# import Parser , commandHandler


@dataclass
class Config:
    host: str = "127.0.0.1"
    port: int = 6379
    rdb_path: Optional[str] = None


async def load_config() -> Config:
    config = Config()
    if '--host' in sys.argv:
        config.host = sys.argv[sys.argv.index('--host') + 1]
    if '--port' in sys.argv:
        config.port = int(sys.argv[sys.argv.index('--port') + 1])
    if '--dir' in sys.argv and '--dbfilename' in sys.argv:
        config.rdb_path = os.path.join(sys.argv[sys.argv.index('--dir') + 1],
                                       sys.argv[sys.argv.index('--dbfilename') + 1])

    return config


async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    try:
        while True:
            command = await Parser.parse(reader)
            if not command:
                break
            await commandHandler.process_command(writer, command)

    except Exception as err:
        print(f"an error {err} occurred with {addr}")
    finally:
        writer.close()
        await writer.wait_closed()


async def load_rdb(rdb_path: str):
    if (rdb_path is not None) and os.path.exists(rdb_path):
        try:
            reader = DBReader(rdb_path)
            reader.read_rdb()
            print('we finished loading the rdb file')
        except Exception as err:
            print(f'error loading rdb file: {err}')


async def main():
    config = await load_config()
    await load_rdb(config.rdb_path)
    asyncio.create_task(active_deleting(expires, hash_table, 20, 25))
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w),
        "127.0.0.1",
        config.port
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
## i need to make a different function to scale the hashtable not pass it in constructor
