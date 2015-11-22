__author__ = 'bulat'
import os.path
from database.table import Table
from relations.student import student
from Connection import  Connection
from database.citadel_db import Database

class driver:

    db = None
    tables = []

    def connect(self):

        # TODO : remove this
        # here should connect to database file
        db_exits = os.path.exists('storage/student.txt')
        print(db_exits)

        if db_exits:
            self.db = Database()
            self.db.add_table(Table(type=student, from_dump=True))
            print("Database already exist. Working with dumb")

        print("Connected to database ")
        return Connection(self.db)

