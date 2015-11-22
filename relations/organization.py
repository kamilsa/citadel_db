__author__ = 'kamil'

"""
class of entity organization
"""


class organization:
    __name__ = 'organization'

    __attrs__ = ['id', 'name']

    def __init__(self, id=0, name="", to_parse=None):
        if to_parse is None:
            self.attrs = {}
            self.attrs['id'] = id
            self.attrs['name'] = name
        else:
            toks = to_parse.split('$')
            # print('to parse = ', to_parse)
            self.attrs = {}
            self.attrs['id'] = toks[1]
            self.attrs['name'] = toks[2]

    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size - len(string)):
            res += ' '
        return res

    """
    :returns string in the following format:
    <number of key attribute> $ <id offset> $ <name offset> $ <cited_name offset> $ <address offset> $
    <id> $ <name> $ <cited_name> $ <address>
    """

    def get_string(self):
        res = ''
        res += self.add_spaces_to_size(str(1), 2) + '$'
        res += str(self.attrs['id']) + '$'
        res += str(self.attrs['name']) + '$'
        return res

    def get_key(self):
        return self.attrs['id']
