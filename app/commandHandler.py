from app.HashTable import HashTable
import time
from app import hash_table, expires


async def process_echo_command(writer, word):
    msg = f"${len(word)}\r\n{word}\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_ping_command(writer):
    msg = f"$4\r\nPONG\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_set_command(writer, key, value):
    hash_table.set(key, value)
    msg = f"+OK\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_set_px_command(writer, key, value, offset: int):
    hash_table.set(key, value)
    exiprationTime = time.time() * 1000 + offset
    expires.set(key, exiprationTime)
    print(f"expires: {exiprationTime} , offset: {offset}")
    print()
    msg = f"+OK\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_get_command(writer, key):
    expires_value = expires.get(key)
    msg = f"$-1\r\n"
    print(f"expires: {expires_value} , current time: {time.time() * 1000}")
    if expires_value is not None and expires_value < time.time() * 1000 + 5:
        expires.delete(key)
        hash_table.delete(key)
        print("ahahah")
    value = hash_table.get(key)
    if value is not None:
        msg = f"${len(value)}\r\n{value}\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_command(writer, commands):
    i = 0
    while i < len(commands):
        if commands[i].lower() == "echo":
            if i + 1 >= len(commands):
                raise Exception("ERR wrong number of arguments for 'echo' command")
            await process_echo_command(writer, commands[i + 1])
            i += 1
        elif commands[i].lower() == "ping":
            await process_ping_command(writer)
        elif commands[i].lower() == "set" and i + 4 < len(commands) and commands[i + 3].lower() == "px":
            await process_set_px_command(writer, commands[i + 1], commands[i + 2], int(commands[i + 4]))
            i += 4
        elif commands[i].lower() == "set":
            if i + 2 >= len(commands):
                raise Exception("ERR wrong number of arguments for 'set' command")
            await process_set_command(writer, commands[i + 1], commands[i + 2])
            i += 2
        elif commands[i].lower() == "get":
            if i + 1 >= len(commands):
                raise Exception("ERR wrong number of arguments for 'get' command")
            await process_get_command(writer, commands[i + 1])
            i += 1
        i += 1
