import socket

import asyncio
from . import Parser
from . import commandHandler
from .expire import active_deleting
from app import expires, hash_table

# import Parser , commandHandler


HOST = "localhost"
PORT = 6379
BUFFER_SIZE = 1024


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


async def main():
    asyncio.create_task(active_deleting(expires, hash_table, 20, 25))
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w),
        "127.0.0.1",
        6379
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
