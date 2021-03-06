__author__ = 'kamil'


class page:
    def __init__(self, page_offset=None, filename=None):
        self.count = 0  # number of records
        self.total_space = 4000
        self.occupied_space = 400
        self.lengths = []
        self.offsets = []
        self.page_str = self.add_spaces_to_size('header0  $400  $0  $#', 400)
        self.end_pointer = 400
        self.header_offset = 400
        self.d = 0
        self.page_offset = page_offset
        self.page_str = self.add_spaces_to_size(self.page_str, 400)
        if filename != None:
            f = open(filename, 'rb+')
            f.seek(page_offset)
            self.page_str = f.read(4000)
            f.close()
            if self.page_str[:len('header')] != 'header':
                self.page_str = 'header0  $400  $0  $#'
                self.page_str = self.add_spaces_to_size(self.page_str, 400)
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
        self.offsets.append(self.end_pointer)
        pos = len(self.page_str.split('#')[0])
        self.page_str = self.put_to_string(self.page_str, pos, self.add_spaces_to_size(str(self.end_pointer),
                                                                                       3) + ',' + self.add_spaces_to_size(
            str(len(ent_string)), 3) + '$#')

        # update end pointer
        old_end_pointer = self.end_pointer
        self.end_pointer += len(ent_string)
        pos = len('header???$')
        self.page_str = self.put_to_string(self.page_str, pos, self.add_spaces_to_size(str(self.end_pointer), 5))

        self.page_str = self.put_to_string(self.page_str, old_end_pointer, ent_string)

        # print(self.page_str)

    def delete(self, key, attr_numb):
        def spaces(n):
            res = ''
            for i in range(n):
                res += ' '
            return res

        prev = self.header_offset
        for i in range(len(self.lengths)):
            record_str = self.page_str[prev:prev + self.lengths[i]]
            toks = record_str.split('$')
            if toks[attr_numb].strip() == key.strip():
                # print(self.page_str.split('#')[0])

                oldoffset = self.offsets[i]
                oldlen = self.lengths[i]

                pos = len('header???$?????$???$') + 8 * i
                self.page_str = self.page_str[:pos] + self.page_str[pos + 8:]
                self.lengths.remove(self.lengths[i])
                self.offsets.remove(self.offsets[i])
                newoffsets = self.offsets[:i]
                for j in range(i,len(self.offsets)):
                    newoffsets.append(self.offsets[j] - oldlen)

                self.offsets = newoffsets
                offs_lens = ""
                for j in range(len(self.offsets)):
                    offs_lens += str(self.offsets[j]) + ',' + str(self.lengths[j]) + '$'
                offs_lens += '#'
                pos = len('header???$?????$???$')
                self.page_str = self.put_to_string(self.page_str, pos, offs_lens)
                pos = self.page_str.find('#')+1
                self.page_str = self.page_str[:pos] + spaces(8) + self.page_str[pos:]
                self.page_str = self.page_str[:oldoffset] + self.page_str[oldoffset+oldlen:]
                self.end_pointer -= oldlen
                self.count -= 1
                pos = len('header')
                self.page_str = self.put_to_string(self.page_str, pos,
                                                   self.add_spaces_to_size(str(len(self.offsets)), 3))
                pos = len('header???$')
                self.page_str = self.put_to_string(self.page_str, pos,
                                                   self.add_spaces_to_size(str(self.end_pointer), 5))

                # self.page_str = self.put_to_string(self.page_str, pos, self.add_spaces_to_size(str(-1), 3))
                return record_str
            prev += self.lengths[i]

    def get(self, key, attr_numb):
        prev = self.header_offset
        for length in self.lengths:
            record_str = self.page_str[prev:prev + length]
            toks = record_str.split('$')
            if toks[attr_numb].strip() == key.strip():
                return record_str
            prev += length

    # returns array of records in page
    def items(self):
        res = []
        prev = self.header_offset
        for length in self.lengths:
            res.append(self.page_str[prev:prev + length])
            prev += length
        return res

    def set_doubling(self, d):
        self.d = d
        pos = len('header???$?????$')
        self.page_str = self.put_to_string(self.page_str, pos, self.add_spaces_to_size(str(d), 3))

    def get_available_space(self):
        sum = 400
        for length in self.lengths:
            if (length != -1):
                sum += length

        return self.total_space - sum

    def is_fit(self, entity):
        if self.get_available_space() - len(entity.get_string()) >= 0:
            return True
        else:
            return False

    def store(self, filename, offset):
        self.page_offset = offset
        f = open(filename, 'rb+')
        f.seek(offset)
        f.write(self.add_spaces_to_size(self.page_str, self.total_space))
        f.close()
