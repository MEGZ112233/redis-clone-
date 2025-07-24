from dataclasses import asdict
from app.main import Config, load_config, Replication_info, load_replication_info
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
    msg = f"+PONG\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_set_command(writer, key, value):
    hash_table.set(key, value)
    msg = f"+OK\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_set_px_command(writer, key, value, offset: int):
    hash_table.set(key, value)
    expiration_time = time.time() * 1000 + offset
    expires.set(key, expiration_time)
    print(f"expires: {expiration_time} , offset: {offset}")
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
    config = await load_config()
    config_dict = asdict(config)
    if command.lower() == "get":
        msg = formatArray([key, config_dict[key]])
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


async def process_info(writer, section):
    if section is None:
        pass
        ## i need to make a different function to represent all the info because we don't have all the info

    replication_info_dict = asdict(await load_replication_info())
    atributes_info = '\r\n'.join(f"{key}:{value}" for key, value in replication_info_dict.items())
    if section.lower() == "replication":
        msg = formatBulkString(atributes_info)
    print(f"the replication info is {msg}")
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
                i += 2
                continue
            elif commands[i].lower() == "ping":
                await process_ping_command(writer)
                i += 1
                continue
            elif commands[i].lower() == "set" and i + 4 < len(commands) and commands[i + 3].lower() == "px":
                await process_set_px_command(writer, commands[i + 1], commands[i + 2], int(commands[i + 4]))
                i += 5
                continue
            elif commands[i].lower() == "set":
                if i + 2 >= len(commands):
                    raise Exception("ERR wrong number of arguments for 'set' command")
                await process_set_command(writer, commands[i + 1], commands[i + 2])
                i += 3
                continue
            elif commands[i].lower() == "get":
                if i + 1 >= len(commands):
                    raise Exception("ERR wrong number of arguments for 'get' command")
                await process_get_command(writer, commands[i + 1])
                i += 2
                continue
            elif commands[i].lower() == "config":
                if i + 2 >= len(commands):
                    raise Exception("ERR wrong number of arguments for 'config' command")
                await process_config(writer, commands[i + 1], commands[i + 2])
                i += 3
                continue
            elif commands[i].lower() == "keys":
                if i + 1 >= len(commands):
                    raise Exception("ERR wrong number of arguments for 'KEYS' command")
                pattern = commands[i + 1]
                print(f"the pattern is {pattern}")
                i += 2
                await process_keys(writer, pattern)
                continue
            elif commands[i].lower() == "info":
                section = None
                if i + 1 < len(commands):
                    section = commands[i + 1]
                await process_info(writer, section)
                i += 2
                continue

    except Exception as err:
        print(err)
        writer.write(formatError()).encode()
        await writer.drain()
