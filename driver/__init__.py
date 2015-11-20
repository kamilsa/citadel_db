__author__ = 'bulat'
import os.path
from database.hash_db import hash_db
from relations.student import student

class driver:

    db_exists = False

    def connect(self):
        self.db_exits = os.path.isfile("/storage/student.txt")
        if self.db_exits:
            self.db = hash_db(type=student, from_dump=True)
        print("Connected to database ")


    def execute(self, query):
        pass

    # c = cursor(db=db, filename=db.filename)
    # c = select_cursor(db=db,filename=db.filename, on_field='name', greater_than="G", less_than="K")
    c = project_cursor(db=db,filename=db.filename, fields={'name', 'email'})
    while c.has_next():
        print c.next()

