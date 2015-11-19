__author__ = 'kamil'

"""
class of entity pair
"""
class pair:

    __name__ = 'pair'

    def __init__(self, id = 0, id1 = 0, id2 =0, to_parse = None):
        if to_parse == None:
            self.attrs = {}
            self.attrs['id'] = id
            self.attrs['id1'] = id1
            self.attrs['id2'] = id2
        else:
            toks = to_parse.split('$')
            # print('to parse = ', to_parse)
            self.attrs = {}
            self.attrs['id^'] = toks[1]
            self.attrs['id1'] = toks[2]
            self.attrs['id2'] = toks[3]
    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size-len(string)):
            res += ' '
        return res

    """
    :returns string in the following format:
    <number of key attribute> $ <id offset> $ <name offset> $ <email offset> $ <address offset> $
    <id> $ <name> $ <email> $ <address>
    """
    def get_string(self):
        res = ''
        res += self.add_spaces_to_size(str(1), 2) + '$'
        # res += self.add_spaces_to_size(str(id_offset),2) + '$'
        # res += self.add_spaces_to_size(str(name_offset),2) + '$'
        # res += self.add_spaces_to_size(str(email_offset),2) + '$'
        # res += self.add_spaces_to_size(str(address_offset),2) + '$'
        res += str(self.attrs['id']) + '$'
        res += str(self.attrs['id1']) + '$'
        res += str(self.attrs['id2']) + '$'
        return res

    def get_key(self):
        return self.attrs['id']