__author__ = 'kamil'

page_size = 4000

class Ipage:
    def __init__(self, page_offset=None, filename=None):
        self.count = 0  # number of records
        self.total_space = page_size
        self.occupied_space = 0
        self.lengths = []
        self.offsets = []
        self.page_str = self.add_spaces_to_size('header0  $4000 $0  $#', 4000)
        self.end_pointer = page_size
        self.header_offset = len(self.page_str)
        self.d = 0
        self.page_offset = page_offset
        if filename is not None:
            f = open(filename, 'rb+')
            f.seek(page_offset)
            self.page_str = self.add_spaces_to_size(f.read(self.total_space), 4000)
            f.close()
            if self.page_str[:len('header')] != 'header':
                self.page_str = self.add_spaces_to_size('header0  $4000 $0  $#', 4000)
            header_str = self.page_str.split('#')[0]
            toks = header_str.split('$')
            self.count = int(toks[0][len('header'):])
            self.end_pointer = int(toks[1])
            self.d = int(toks[2])
            for offset_length in toks[3:]:
                if offset_length != '':
                    self.offsets.append(int(offset_length.split(',')[0]))
                    self.lengths.append(int(offset_length.split(',')[1]))

    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size - len(string)):
            res += ' '
        return res

    def put_to_string(self, old, pos, to_put):
        res = old[:pos] + to_put + old[pos + len(to_put):]
        return res

    def insert(self, entity):
        to_proc = self.page_str.split('#')[0] + '#'
        ent_string = entity.get_string()

        # increase counter
        self.count += 1
        self.page_str = self.put_to_string(self.page_str, len('header'), self.add_spaces_to_size(str(self.count), 3))

        # update the offsets and lengths
        self.lengths.append(len(ent_string))
        self.offsets.append(self.end_pointer - len(ent_string))
        pos = len(self.page_str.split('#')[0])
        self.page_str = self.put_to_string(self.page_str, pos, self.add_spaces_to_size(str(self.end_pointer),
                                                                                       3) + ',' + self.add_spaces_to_size(
            str(len(ent_string)), 3) + '$#')

        # update end pointer
        old_end_pointer = self.end_pointer
        self.end_pointer -= len(ent_string)
        pos = len('header???$')
        self.page_str = self.put_to_string(self.page_str, pos, self.add_spaces_to_size(str(self.end_pointer), 5))

        self.page_str = self.put_to_string(self.page_str, old_end_pointer - len(ent_string), ent_string)

        # print(self.page_str)

    def get(self, key, attr_numb):
        prev = self.header_offset
        for length in self.lengths:
            record_str = self.page_str[prev - length:prev]
            toks = record_str.split('$')
            if toks[attr_numb].strip() == key.strip():
                return record_str
            prev -= length

    # returns array of records in page
    def items(self):
        res = []
        prev = self.header_offset
        for length in self.lengths:
            res.append(self.page_str[prev - length:prev])
            prev -= length
        return res

    def set_doubling(self, d):
        self.d = d
        pos = len('header???$?????$')
        self.page_str = self.put_to_string(self.page_str, pos, self.add_spaces_to_size(str(d), 3))

    def get_available_space(self):
        if self.count == 0:
            return 999999
        res = 0
        toks = self.page_str.split('#')
        count = self.end_pointer - len(toks[0]) - 12
        res += count
        return res

    def is_fit(self, entity):
        if self.get_available_space() - len(entity.get_string()) >= 0:
            return True
        else:
            return False

    def store(self, filename, offset):
        self.page_offset = offset
        f = open(filename, 'rb+')
        f.seek(offset)
        f.write(self.page_str)
        f.close()

    def store_to_tree(self, tree, entity, attr_index, filename):
        # print zip(self.lengths, self.offsets)
        for item, length, offset in zip(self.items(), self.lengths, self.offsets):
            carriage = self.page_offset + offset
            ent = entity(to_parse=item)
            if tree.get(ent.attrs[attr_index]) is None:
                tree[ent.attrs[attr_index]] = set()
            tree[ent.attrs[attr_index]].add(filename + ',' + str(carriage - length) + ',' + str(length))
        tree.commit()
