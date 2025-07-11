import socket  # noqa: F401

import asyncio
import Parser
HOST = "localhost"
PORT = 6379
BUFFER_SIZE = 1024

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    try:
        while True:
            request = await reader.read(BUFFER_SIZE)
            if not request:
                break
            msg = request.decode()
            if "ping" in msg.lower():
                msg = b"+PONG\r\n"
                writer.write(msg)
                await writer.drain()
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
