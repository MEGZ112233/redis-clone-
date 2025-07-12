async def process_echo_command(writer, word):
    msg = f"${len(word)}\r\n{word}\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_ping_command(writer):
    msg = f"$4\r\nPONG\r\n"
    writer.write(msg.encode())
    await writer.drain()


async def process_command(writer, commands):
    i = 0
    while i < len(commands):
        ## in redis it can be echo or Echo or anything

        if commands[i].lower() == "echo":
            if i + 1 >= len(commands):
                raise Exception("ERR wrong number of arguments for 'echo' command")
            await process_echo_command(writer, commands[i + 1])
            i += 1
        elif commands[i].lower() == "ping":
            await process_ping_command(writer)

        i += 1
