import app
from app.HashTable import HashTable


def check_bit(number: int, idx: int):
    return (number >> idx) & 1 == 1


class DBReader:

    def __init__(self, file_path):
        self.file_path = file_path
        self.file = None
        self.hash_table_size = 0
        self.expire_table_size = 0
        self.meta_data = []

    def create_database(self):
        if self.hash_table_size > 0:
            app.hash_table.clear_resize(self.hash_table_size)
        if self.expire_table_size > 0:
            app.expires.clear_resize(self.expire_table_size)

    def read_rdb(self):
        with open(self.file_path, 'rb') as f:
            self.file = f
            magic_string = f.read(9)
            is_valid_magic = self.verify_magic_string(magic_string)
            if not is_valid_magic:
                raise Exception("Invalid file format")
            print('we finished verifying the magic string')
            while True:
                byte = self.file.read(1)
                ## i was compare byte with number which will never be right
                print(f'the byte op code is {byte}')
                if byte is None:
                    break
                op_code = byte[0]
                if op_code == 0xFA:
                    print('meta data part')
                    key = self.read_string_encoding()
                    value = self.read_string_encoding()
                    self.meta_data.append([key, value])
                    print(f"the key is {key} and the value is {value}")
                    continue
                elif op_code == 0xFE:
                    print(f'data base index part')
                    DB_index = self.read_length_encoding()
                    print(f"the db index is {DB_index}")
                    continue
                elif op_code == 0xFB:
                    print("database size part")
                    self.hash_table_size, _ = self.read_length_encoding()
                    self.expire_table_size, _ = self.read_length_encoding()
                    print(
                        f"the hash table size is {self.hash_table_size} and the expire table size is {self.expire_table_size}")
                    self.create_database()
                    continue
                elif op_code == 0xFD:
                    print('expire part in seconds')
                    expire_time = self.read_number(4) * 1000
                    typ = self.file.read(1)[0]
                    key, value = self.read_key_value_pair(typ)
                    print(f"the expire time is {expire_time} and the key is {key} and the value is {value}")
                    app.expires.set(key, expire_time)
                    app.hash_table.set(key, value)
                    continue
                elif op_code == 0xFC:
                    print('expire part in ms')
                    expire_time = self.read_number(8)
                    typ = self.file.read(1)[0]
                    key, value = self.read_key_value_pair(typ)
                    app.expires.set(key, expire_time)
                    app.hash_table.set(key, value)
                    continue
                elif op_code == 0x00:
                    print('normal key value pair')
                    key, value = self.read_key_value_pair(0)
                    app.hash_table.set(key, value)
                    continue
                elif op_code == 0xFF:
                    print('end of file')
                    self.file.read(8)
                    break
                else:
                    raise Exception("Invalid i don't know any other op code")

    def verify_magic_string(self, magic_string):
        if magic_string == b"REDIS0011":
            return True
        raise Exception("Invalid file format")

    ## this must be more dynamic we should save the redis version also

    def read_key_value_pair(self, typ):

        if typ == 0:
            key = self.read_string_encoding()
            value = self.read_string_encoding()
            return key, value
        else:
            raise Exception("Invalid: I don't know any other value of key-value pair")

    def read_number(self, num_of_bytes):
        ## it uses little endian method
        value = 0

        for i in range(num_of_bytes):
            byte_value = self.file.read(1)[0]
            value = value + (byte_value << (i * 8))
        return value

    def read_length_encoding(self):
        length = 0
        parseCode = 0
        ## if type 0 then it's the length of the string else is the type of parsing like 0xC0 OR 0xC1

        indicator = self.file.read(1)[0]
        if not check_bit(indicator, 7) and not check_bit(indicator, 6):
            length = indicator
        elif not check_bit(indicator, 7) and check_bit(indicator, 6):
            indicator = indicator - (1 << 6)
            length = (self.file.read(1)[0] << 8) | indicator
        elif check_bit(indicator, 7) and not check_bit(indicator, 6):
            indicator = indicator - (1 << 7)
            for i in range(4):
                length = (length << 8) + (self.file.read(1)[0])
        else:
            parseCode = indicator

        return length, parseCode

    def read_string_encoding(self):
        length, parseCode = self.read_length_encoding()
        if parseCode == 0:
            value = self.file.read(length).decode('utf-8')
        elif parseCode == 0xC0:
            value = str(self.read_number(1))
        elif parseCode == 0xC1:
            value = str(self.read_number(2))
        elif parseCode == 0xC2:
            value = str(self.read_number(4))
        else:
            raise Exception(" encoding Error type at string Encoding ")
        return value
