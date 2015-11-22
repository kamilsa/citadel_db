__author__ = 'kamil'

"""
class of entity article_citation
"""


class article_citation:
    __name__ = 'article_citation'

    __attrs__ = ['id', 'article_id', 'cited_article_id']

    def __init__(self, id=0, article_id="", cited_article_id="", to_parse=None):
        if to_parse is None:
            self.attrs = {}
            self.attrs['id'] = id
            self.attrs['article_id'] = article_id
            self.attrs['cited_article_id'] = cited_article_id
        else:
            toks = to_parse.split('$')
            # print('to parse = ', to_parse)
            self.attrs = {}
            self.attrs['id'] = toks[1]
            self.attrs['article_id'] = toks[2]
            self.attrs['cited_article_id'] = toks[3]

    # returns version of string with added spaces
    def add_spaces_to_size(self, string, size):
        res = string
        for i in range(0, size - len(string)):
            res += ' '
        return res

    """
    :returns string in the following format:
    <number of key attribute> $ <id offset> $ <article_id offset> $ <cited_article_id offset> $ <address offset> $
    <id> $ <article_id> $ <cited_article_id> $ <address>
    """

    def get_string(self):
        res = ''
        res += self.add_spaces_to_size(str(1), 2) + '$'
        res += str(self.attrs['id']) + '$'
        res += str(self.attrs['article_id']) + '$'
        res += str(self.attrs['cited_article_id']) + '$'
        return res

    def get_key(self):
        return self.attrs['id']
