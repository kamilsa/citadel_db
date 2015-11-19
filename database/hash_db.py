import hashlib
import os
import pickle
from mx.BeeBase import BeeDict
from database.ipage import ipage

__author__ = 'kamil'


class container:
    filename = 0
    gd = 0
    pp = 0
    type = 0
    counter = 0
    size = 0
    m = 0
    index_attrs = None
    key_sizes = None


class hash_db:
    def __init__(self, filename=None, type=None, index_attrs=None, key_sizes=None, from_dump=False):
        if from_dump == True:
            path = os.getcwd() + '/storage/' + type.__name__ + '/'
            c = pickle.load(open(path + 'dumb.p', 'rb'))
            self.filename = c.filename
            self.gd = c.gd
            self.pp = c.pp
            self.type = c.type
            self.counter = c.counter
            self.size = c.size
            self.m = c.m
            self.index_attrs = c.index_attrs
            self.key_sizes = c.key_sizes
            self.trees = {}
            for index_attr, key_size in zip(self.index_attrs, self.key_sizes):
                tree = BeeDict.BeeStringDict(os.getcwd() + '/storage/' + type(type).__name__ + index_attr,
                                                 keysize=key_size)
                # tree.close()
                self.trees[index_attr] = tree
        else:
            self.filename = filename
            self.gd = 0
            self.pp = [0]
            self.type = type
            self.counter = 0
            self.size = 0
            self.m = {}
            if index_attrs is not None and key_sizes is not None:
                self.index_attrs = index_attrs
                self.key_sizes = key_sizes
                self.trees = {}
                for attr, key_size in zip(index_attrs, key_sizes):
                    tree = BeeDict.BeeStringDict(os.getcwd() + '/storage/' + type(type).__name__ + attr,
                                                 keysize=key_size)
                    # tree.close()
                    self.trees[attr] = tree

    def my_hash(self, a_str):
        a_str = str(a_str)
        a_str = a_str.encode('utf8')
        return int(hashlib.md5(a_str).hexdigest(), 16)

    def get_page(self, k):
        k = int(k)
        h = self.my_hash(k)
        # h = hash(k)
        offset = self.pp[h & ((1 << self.gd) - 1)]
        return ipage(offset, self.filename)

    def put(self, k, v):
        for attr, key_size in zip(self.index_attrs, self.key_sizes):
            tree = self.trees[attr]
            tree[v.attrs[attr]] = v.get_string()

            tree.commit()

        p = self.get_page(k)
        if p.is_fit(v) == False and p.d == self.gd:
            self.pp = self.pp + self.pp
            self.gd += 1
        if p.is_fit(v) == False and p.d < self.gd:
            # p.insert(v)
            p1 = ipage()
            p2 = ipage()
            items = p.items()
            items.append(v.get_string())
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
                        p1 = ipage()
                        p2 = ipage()
                        if p.d == self.gd:
                            first = False
                    else:
                        print(len(self.pp))
                        p.d = self.gd
                        self.pp *= 2
                        self.gd += 1
                        p1 = ipage()
                        p2 = ipage()
                else:
                    break

            self.counter += 1
            for i, x in enumerate(self.pp):
                if x == p.page_offset:
                    if (i >> p.d) & 1 == 1:
                        self.pp[i] = self.counter * p.total_space
                    else:
                        self.pp[i] = p.page_offset
            p1.set_doubling(p.d + 1)
            p2.set_doubling(p1.d)

            p1.store(self.filename, p.page_offset)
            p2.store(self.filename, self.counter * p.total_space)

        else:
            p.insert(v)
            p.store(self.filename, p.page_offset)

        # if self.check() == False:
        #     pass
        self.size += 1

    def remove(self, key, attr_number):
        stud = self.type(to_parse = self.get(key, attr_number))
        for attr, key_size in zip(self.index_attrs, self.key_sizes):
            tree = self.trees[attr]
            print 'here', stud.attrs
            del tree[stud.attrs[attr]]
            tree.commit()
        p = self.get_page(key)
        p.delete(key, attr_number)
        p.store(self.filename, p.page_offset)
        self.size -= 1

    def update(self, key, attr_number, up_fields):
        for a_key in up_fields.keys():
            if '^' in a_key:
                print "You cannot update key!"
                return
        record = self.type(to_parse = self.get(key, attr_number))
        self.remove(key, attr_number)
        for a_key, value in zip(up_fields.keys(), up_fields.values()):
            record.attrs[a_key] = value
        self.put(record.get_key(), record)
        for attr, key_size in zip(self.index_attrs, self.key_sizes):
            tree = self.trees[attr]
            tree[record.attrs[attr]] = record.get_string()
            tree.commit()

    def check(self):
        f = open(self.filename, 'rb')
        for offset in self.pp:
            f.seek(offset)
            f.read(len('header???$'))
            end_pointer = int(f.read(3))
            if end_pointer > 2000:
                f.close()
                return False
        f.close()
        return True

    def get(self, key, attr_numb):
        p = self.get_page(key)
        return p.get(key, attr_numb)

    def save(self):
        path = os.getcwd() + '/storage/' + self.type.__name__ + '/'
        c = container()
        c.filename = self.filename
        c.gd = self.gd
        c.pp = self.pp
        c.type = self.type
        c.counter = self.counter
        c.size = self.size
        c.m = self.m
        if self.index_attrs is not None and self.key_sizes is not None:
            c.index_attrs = self.index_attrs
            c.key_sizes = self.key_sizes
        pickle.dump(c, open(path + 'dumb.p', 'wb'))

    def neighbours(self, key_name, key, n, inclusive = True):
        tree = self.trees[key_name]
        curr = tree.cursor(key = key)
        res = []
        if inclusive:
            res.append(self.type(to_parse = tree[curr.key]))
        i = 0
        while i < n:
            if not curr.next():
                break
            res.append(self.type(to_parse = tree[curr.key]))
            i += 1
        return res