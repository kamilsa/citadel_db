__author__ = 'bulat'

from sqlparser import _parse

class Cursor:
    arraysize = 0
    pointer = None
    _limit = -1

    def __init__(self, db):
        self.db = db

    def execute(self, query):
        self.pointer, self._limit = _parse(query, self.db)

    def close(self):
        pass

    def fetchone(self):
        if self.pointer.has_next():
            return self.pointer.next()

    def fetchmany(self, size=arraysize):
        temp = []
        proceed_size = 0
        if self._limit!=-1:
            proceed_size = min(self._limit, size)
        else:
            proceed_size = size
        for i in range(proceed_size):
            if self.pointer.has_next():
                temp.append(self.pointer.next())
        return temp

    def fetchall(self):
        temp = []
        if self._limit!=-1:
            for i in range(self._limit):
                if self.pointer.has_next():
                    temp.append(self.pointer.next())
            self.refresh()
        else:
            while self.pointer.has_next():
                temp.append(self.pointer.next())
            self.refresh()
        print "OMG THE SIZE IS ", len(temp)
        return temp


    def refresh(self):
        self.pointer.refresh()
