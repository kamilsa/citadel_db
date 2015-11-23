__author__ = 'bulat'
import os.path
from database.table import Table
from relations.student import student
from Connection import  Connection
from database.citadel_db import Database

class driver:

    db = None
    tables = []

    def __init__(self):
        self.db = Database()

    def add_to_database(self, types):
        self.db.add_table(Table(type=types, from_dump=True))


    def connect(self):

        # TODO : remove this
        # here should connect to database file

        print("Connected to database ")
        return Connection(self.db)

