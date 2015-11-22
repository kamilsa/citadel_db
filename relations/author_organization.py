__author__ = 'kamil'

"""
class of entity author_organization
"""


class author_organization:
    __name__ = 'author_organization'

    __attrs__ = ['id', 'author_id', 'organization_id']

    def __init__(self, id=0, author_id="", organization_id="", to_parse=None):
        if to_parse is None:
            self.attrs = {}
            self.attrs['id'] = id
            self.attrs['author_id'] = author_id
            self.attrs['organization_id'] = organization_id
        else:
            toks = to_parse.split('$')
            # print('to parse = ', to_parse)
            self.attrs = {}
            self.attrs['id'] = toks[1]
            self.attrs['author_id'] = toks[2]
            self.attrs['organization_id'] = toks[3]

    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size - len(string)):
            res += ' '
        return res

    """
    :returns string in the following format:
    <number of key attribute> $ <id offset> $ <author_id offset> $ <organization_id offset> $ <address offset> $
    <id> $ <author_id> $ <organization_id> $ <address>
    """

    def get_string(self):
        res = ''
        res += self.add_spaces_to_size(str(1), 2) + '$'
        res += str(self.attrs['id']) + '$'
        res += str(self.attrs['author_id']) + '$'
        res += str(self.attrs['organization_id']) + '$'
        return res

    def get_key(self):
        return self.attrs['id']
