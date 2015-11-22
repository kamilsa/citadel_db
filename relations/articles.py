__author__ = 'kamil'

"""
class of entity article
"""


class article:
    __name__ = 'article'

    __attrs__ = ['id', 'title', 'year']

    def __init__(self, id=0, title="", year="", to_parse=None):
        if to_parse is None:
            self.attrs = {}
            self.attrs['id'] = id
            self.attrs['title'] = title
            self.attrs['year'] = year
        else:
            toks = to_parse.split('$')
            # print('to parse = ', to_parse)
            self.attrs = {}
            self.attrs['id'] = toks[1]
            self.attrs['title'] = toks[2]
            self.attrs['year'] = toks[3]

    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size - len(string)):
            res += ' '
        return res

    """
    :returns string in the following format:
    <number of key attribute> $ <id offset> $ <title offset> $ <year offset> $ <address offset> $
    <id> $ <title> $ <year> $ <address>
    """

    def get_string(self):
        res = ''
        res += self.add_spaces_to_size(str(1), 2) + '$'
        res += str(self.attrs['id']) + '$'
        res += str(self.attrs['title']) + '$'
        res += str(self.attrs['year']) + '$'
        return res

    def get_key(self):
        return self.attrs['id']