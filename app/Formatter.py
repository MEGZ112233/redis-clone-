def formatBulkString(word):
    msg = f"${len(word)}\r\n{word}\r\n"
    return msg


def formatArray(array):
    if len(array) == 0:
        return formatError()
    msg = f"*{len(array)}\r\n"
    for word in array:
        msg += formatBulkString(word)
    return msg


def formatError():
    msg = f"-1\r\n"
    return msg
