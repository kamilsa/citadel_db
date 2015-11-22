__author__ = 'kamil'

"""
class of entity key_term
"""


class key_term:
    __name__ = 'key_term'

    __attrs__ = ['id', 'term']

    def __init__(self, id=0, term="", to_parse=None):
        if to_parse is None:
            self.attrs = {}
            self.attrs['id'] = id
            self.attrs['term'] = term
        else:
            toks = to_parse.split('$')
            # print('to parse = ', to_parse)
            self.attrs = {}
            self.attrs['id'] = toks[1]
            self.attrs['term'] = toks[2]

    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size - len(string)):
            res += ' '
        return res

    """
    :returns string in the following format:
    <number of key attribute> $ <id offset> $ <term offset> $ <cited_term offset> $ <address offset> $
    <id> $ <term> $ <cited_term> $ <address>
    """

    def get_string(self):
        res = ''
        res += self.add_spaces_to_size(str(1), 2) + '$'
        res += str(self.attrs['id']) + '$'
        res += str(self.attrs['term']) + '$'
        return res

    def get_key(self):
        return self.attrs['id']
