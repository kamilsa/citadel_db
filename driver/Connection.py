__author__ = 'bulat'
from cursor import Cursor

class Connection:

    def __init__(self, db):
        self.db = db

    def close(self):
        pass

    def commit(self):
        pass

    def cursor(self):
        db_cursor = Cursor(self.db)
        return db_cursor

