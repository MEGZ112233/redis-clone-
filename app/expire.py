import asyncio

from .HashTable import HashTable
import time


async def active_deleting(expire_table: HashTable, hash_table: HashTable, k: int, min_effectiveness_percent: int):
    ## the precision is the number of successful exploring attempts

    while True:
        items = expire_table.get_random_keys(k)
        successful_exploring = 0

        for item in items:
            if item[1] < time.time() * 1000:
                expire_table.delete(item[0])
                hash_table.delete(item[0])
                successful_exploring += 1

        if successful_exploring * 100 < min_effectiveness_percent * k:
            break

    await asyncio.sleep(.2)
