__author__ = 'kamil'

"""
class of entity conference_article
"""


class conference_article:
    __name__ = 'conference_article'

    __attrs__ = ['id', 'conference_id', 'article_id']

    def __init__(self, id=0, conference_id="", article_id="", to_parse=None):
        if to_parse is None:
            self.attrs = {}
            self.attrs['id'] = id
            self.attrs['conference_id'] = conference_id
            self.attrs['article_id'] = article_id
        else:
            toks = to_parse.split('$')
            # print('to parse = ', to_parse)
            self.attrs = {}
            self.attrs['id'] = toks[1]
            self.attrs['conference_id'] = toks[2]
            self.attrs['article_id'] = toks[3]

    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size - len(string)):
            res += ' '
        return res

    """
    :returns string in the following format:
    <number of key attribute> $ <id offset> $ <conference_id offset> $ <article_id offset> $ <address offset> $
    <id> $ <conference_id> $ <article_id> $ <address>
    """

    def get_string(self):
        res = ''
        res += self.add_spaces_to_size(str(1), 2) + '$'
        res += str(self.attrs['id']) + '$'
        res += str(self.attrs['conference_id']) + '$'
        res += str(self.attrs['article_id']) + '$'
        return res

    def get_key(self):
        return self.attrs['id']
