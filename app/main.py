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
import random, string


@dataclass
class Config:
    host: str = "127.0.0.1"
    port: int = 6379
    dir: Optional[str] = None
    dbfilename: Optional[str] = None
    role: str = "master"
    master_host: str = "127.0.0.1"
    master_port: int = 6379

    @property
    def rdb_path(self) -> Optional[str]:
        if self.dir is not None or self.dbfilename is not None:
            return os.path.join(self.dir, self.dbfilename)
        else:
            return None


@dataclass
class Replication_info:
    role: str = "master"
    master_replid: str = ""
    master_repl_offset: int = 0


async def load_config() -> Config:
    config = Config()
    if '--host' in sys.argv:
        config.host = sys.argv[sys.argv.index('--host') + 1]
    if '--port' in sys.argv:
        config.port = int(sys.argv[sys.argv.index('--port') + 1])
    if '--dir' in sys.argv:
        config.dir = sys.argv[sys.argv.index('--dir') + 1]
    if '--dbfilename' in sys.argv:
        config.dbfilename = sys.argv[sys.argv.index('--dbfilename') + 1]
    return config


async def load_replication_info() -> Replication_info:
    replication_info: Replication_info = Replication_info()
    if '--replicaof' in sys.argv:
        description = sys.argv[sys.argv.index('--replicaof') + 1]
        master_host, port, *_ = description.split(' ')
        if master_host.lower() != 'no':
            replication_info.role = 'slave'
    replication_info.master_replid = ''.join(random.choice(string.ascii_letters) for _ in range(40))
    return replication_info


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
