import hashlib
from database.page import page

__author__ = 'kamil'


class hash_db:
    def __init__(self, filename, type):
        self.filename = filename
        self.gd = 0
        self.pp = [0]
        self.type = type
        self.counter = 0
        self.size = 0
        self.m = {}

    def my_hash(self, a_str):
        a_str = str(a_str)
        a_str = a_str.encode('utf8')
        return int(hashlib.md5(a_str).hexdigest(), 16)

    def get_page(self, k):
        k = int(k)
        h = self.my_hash(k)
        # h = hash(k)
        offset = self.pp[h & ((1 << self.gd) - 1)]
        return page(offset, self.filename)

    def put(self, k, v):
        p = self.get_page(k)
        if self.size == 2255:
            pass
        if p.is_fit(v) == False and p.d == self.gd:
            self.pp = self.pp + self.pp
            self.gd += 1
        if p.is_fit(v) == False and p.d < self.gd:
            p.insert(v)
            p1 = page()
            p2 = page()
            items = p.items()
            first = True
            while True:
                for record_str in items:
                    entity = self.type(to_parse=record_str)
                    k2 = int(entity.get_key())
                    h = self.my_hash(k2)
                    # h = hash(k2)
                    h = h & ((1 << self.gd) - 1)
                    if (h | (1 << p.d) == h):
                        p2.insert(entity)
                    else:
                        p1.insert(entity)
                if p1.count == 0 or p2.count == 0 or p1.end_pointer > p1.total_space or p2.end_pointer > p2.total_space:
                    print('oops len = ', len(self.pp), ' gd = ', self.gd)
                    if first:
                        p.d += 1
                        p1 = page()
                        p2 = page()
                        if p.d == self.gd:
                            first = False
                    else:
                        print(len(self.pp))
                        p.d = self.gd
                        self.pp *= 2
                        self.gd += 1
                        p1 = page()
                        p2 = page()
                else:
                    break

            self.counter += 1
            for i, x in enumerate(self.pp):
                if x == p.page_offset:
                    if (i >> p.d) & 1 == 1:
                        self.pp[i] = self.counter*p.total_space
                    else:
                        self.pp[i] = p.page_offset
            p1.set_doubling(p.d + 1)
            p2.set_doubling(p1.d)

            p1.store(self.filename, p.page_offset)
            p2.store(self.filename, self.counter*p.total_space)

        else:
            p.insert(v)
            p.store(self.filename, p.page_offset)

        # if self.check() == False:
        #     pass
        self.size += 1

    def remove(self, key, attr_number):
        p = self.get_page(key)
        print(p.page_offset)
        self.size -= 1

    def check(self):
        f = open(self.filename, 'r')
        for offset in self.pp:
            f.seek(offset)
            f.read(len('header???$'))
            end_pointer = int(f.read(3))
            if end_pointer > 1000:
                f.close()
                return False
        f.close()
        return True

    def get(self, key, attr_numb):
        p = self.get_page(key)
        return p.get(key, attr_numb)