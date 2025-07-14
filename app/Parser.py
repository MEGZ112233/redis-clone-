async def parseInt(reader):
    ret = 0
    while True:
        c = await reader.read(1)
        if c == b'\r':
            await reader.read(1)
            return ret
        ret = ret * 10 + int(c.decode())


async def parseBulkString(reader):
    length = await parseInt(reader)
    ret = await reader.read(length)
    ret = ret.decode()
    await reader.read(2)
    return ret


async def parseArray(reader):
    ret = []
    length = await parseInt(reader)
    for i in range(length):
        ret.append(await parse(reader))
    return ret


async def parse(reader):
    choice = await reader.read(1)
    choice = choice.decode()
    if not choice :
        return None
    if choice == '*':
        return await parseArray(reader)
    elif choice == ':':
        return await parseInt(reader)
    elif choice == '$':
        return await parseBulkString(reader)
    else :
        raise Exception(f"invalid choice: {choice}")

## "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
