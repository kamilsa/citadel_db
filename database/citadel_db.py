__author__ = 'bulat'


class Database:
    tables = {}
    num_tables = 0

    def __init__(self):
        pass

    def add_table(self, table):
        self.tables[table.name] = table
        self.num_tables += 1

    def drop_table(self, table):
        del (self.tables[table.name])
