def rotate_left(value, offset):
    return ((value << offset) | (value >> (32 - offset))) & 0xFFFFFFFF


def get_hash(text):
    text =  text.encode('utf-8')
    hashValue = 112233
    c1 = 0xcc9e2d51
    c2 = 0x1b873593
    idx = 0
    while idx < len(text):
        k = 0
        for i in range(min(len(text) - idx, 4)):
            k = k + (text[idx + i] << (8 * i))
        k = k * c1
        k = rotate_left(k, 15)
        k = k * c2
        hashValue = (hashValue ^ k)
        hashValue = rotate_left(hashValue, 13)
        hashValue = (hashValue * 5 + 0xe6546b64) & 0xFFFFFFFF
        idx = idx + 4
    hashValue = hashValue ^ (len(text))
    hashValue = hashValue ^ (hashValue >> 16)
    hashValue = hashValue * 0x85ebca6b
    hashValue = hashValue ^ (hashValue >> 13)
    hashValue = hashValue * 0xc2b2ae35
    hashValue = hashValue ^ (hashValue >> 16)
    return hashValue
