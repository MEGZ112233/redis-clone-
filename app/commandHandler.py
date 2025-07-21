import app
from app.HashTable import HashTable
from app.Formatter import *
from app.expire import is_valid
import time
from app import hash_table, expires
import fnmatch


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
    msg = f"$-1\r\n"
    is_key_valid = await is_valid(expires, hash_table, key)
    if is_key_valid:
        value = hash_table.get(key)
        msg = f"${len(value)}\r\n{value}\r\n"

    writer.write(msg.encode())
    await writer.drain()


async def process_config(writer, command: str, key):
    if command.lower() == "get":
        msg = formatArray([key, app.RDB_map[key]])
    else:
        raise Exception("i don't know what to do with this command")
    writer.write(msg.encode())
    await writer.drain()


async def process_keys(writer, pattern):
    keys = hash_table.get_keys()
    print(f'the current length of the hash_table is {hash_table.get_size()}')
    matched_keys = []
    for key in keys:
        is_key_valid = await is_valid(expires, hash_table, key)
        if fnmatch.fnmatch(key, pattern) and is_key_valid == True:
            matched_keys.append(key)
    msg = formatArray(matched_keys)
    writer.write(msg.encode())
    await writer.drain()


async def process_command(writer, commands):
    print(commands)
    try:
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
            elif commands[i].lower() == "config":
                if i + 2 >= len(commands):
                    raise Exception("ERR wrong number of arguments for 'config' command")
                await process_config(writer, commands[i + 1], commands[i + 2])
                i += 2
            elif commands[i].lower() == "keys":
                if i + 1 >= len(commands):
                    raise Exception("ERR wrong number of arguments for 'KEYS' command")
                pattern = commands[i + 1]
                print(f"the pattern is {pattern}")
                await process_keys(writer, pattern)

            i += 1
    except Exception as err:
        print(err)
        writer.write(formatError()).encode()
        await writer.drain()
