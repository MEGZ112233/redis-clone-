import socket  # noqa: F401

import asyncio
import Parser
import commandHandler
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
    server = await asyncio.start_server(handle_client, HOST, PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
