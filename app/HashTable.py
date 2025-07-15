from .murmurHashing import *
import random


class HashTable:
    def __init__(self, size):
        self.size = size
        self.buckets =  [[] for _ in range(size)]

    def set(self, key, value):
        index = get_hash(key) % self.size
        bucket = self.buckets[index]
        for pair in bucket:
            if pair[0] == key:
                pair[1] = value
                return True
        bucket.append([key, value])
        return True

    def get(self, key):
        index = get_hash(key) % self.size
        bucket = self.buckets[index]
        for pair in bucket:
            if pair[0] == key:
                return pair[1]

        return None

    def delete(self, key):
        index = get_hash(key) % self.size
        bucket = self.buckets[index]
        for i, pair in enumerate(bucket):
            if pair[0] == key:
                del bucket[i]
                return True
        return False

    def get_random_keys(self, k):
        index = random.randint(0, len(self.buckets))
        trial = 0
        items = []
        while trial < len(self.buckets) and len(items) < k:
            index = index % len(self.buckets)
            bucket = self.buckets[index]
            index += 1
            trial += 1
            if len(bucket) == 0:
                continue
            for x in bucket:
                items.append(x)
                if len(items) == k:
                    break
        return items

