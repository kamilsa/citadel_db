# opeations for database
from database.cursor import cursor, select_cursor, project_cursor
from database.hash_db import hash_db


def scan(db):
    return cursor(db, db.filename)


def select(db, on_field, greater_than=None, less_than=None):
    return select_cursor(db, db.filename, greater_than=greater_than, less_than=less_than)


def project(db, on_field, fields=None):
    return project_cursor(db, db.filename, fields=fields)
