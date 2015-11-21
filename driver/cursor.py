__author__ = 'bulat'

import sqlparse
import  database.cursor

def _parse(query, db):
    sqlparse.parse(query)
    parsed = sqlparse.parse(query)[0]
    sql_type =  parsed.get_type()
    # Primitive checking
    if (sql_type == "UNKNOWN" or sql_type == None):
        raise(BaseException("Syntax error in sql query"))

    if sql_type == "SELECT":
        # Select procedure
        proj = parsed.tokens[2]
        print proj
        if proj.is_whitespace() or str(proj) == '*':
            # No projection - full select

          #  c = database.cursor.select_cursor(db=db,filename=db.filename, on_field='name', greater_than=None, less_than="B")
           # c = database.cursor.project_cursor(db=db, filename=db.filename, fields={'name', 'email'}, ordered_on='name')
            c = database.cursor.cursor(db=db, filename=db.filename)
            while c.has_next():
               print c.next()


    elif sql_type == "INSERT":
        pass
   # elif sql_type == ""
    # projection :

class Cursor:

    arraysize = 0

    def __init__(self, db):
        self.db = db

    def execute(self, query):
        _parse(query, self.db)

    def close(self):
        pass

    def fetchone(self):
        pass

    def fetchmany(self, size=arraysize):
        pass

    def fetchall(self):
        pass
