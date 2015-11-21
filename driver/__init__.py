__author__ = 'bulat'
import os.path
from database.hash_db import hash_db
from relations.student import student
from Connection import  Connection

class driver:

    db_exists = False

    def connect(self):
        self.db = None
        # TODO : remove this
        self.db_exits = os.path.exists('storage/student.txt')
        print(self.db_exits)
        if self.db_exits:
            self.db = hash_db(type=student, from_dump=True)
            print("Database already exist. Working with dumb")

        print("Connected to database ")
        return Connection(self.db)


    def execute(self, query):
        pass


