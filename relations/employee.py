__author__ = 'kamil'

"""
class of entity employee
"""


class employee:
    __name__ = 'employee'

    def __init__(self, id=0, name="", designation="", address="", to_parse=None):
        if to_parse == None:
            self.attrs = {}
            self.attrs['id^'] = id
            self.attrs['name'] = name
            self.attrs['designation'] = designation
            self.attrs['address'] = address
        else:
            toks = to_parse.split('$')
            # print('to parse = ', to_parse)
            self.attrs = {}
            self.attrs['id^'] = toks[1]
            self.attrs['name'] = toks[2]
            self.attrs['designation'] = toks[3]
            self.attrs['address'] = toks[4]

    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size - len(string)):
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
        res += str(self.attrs['id^']) + '$'
        res += self.attrs['name'] + '$'
        res += self.attrs['designation'] + '$'
        res += self.attrs['address'] + '$'
        return res

    def get_key(self):
        return self.attrs['id^']
