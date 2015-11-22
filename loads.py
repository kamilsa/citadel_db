import os

import psycopg2
from database.table import Table
from relations.author_name import author_name


def load_author_names():
    conn = psycopg2.connect("dbname='citadel' user='bulat' host='localhost' password=''")
    cur = conn.cursor()
    cur.execute("select * from author_names")
    rows = cur.fetchall()
    print ("\nShow me the databases:\n")
    from relations.edges import edge
    open(os.getcwd() + '/storage/edge.txt', 'wb').close()
    db = Table(filename=os.getcwd() + '/storage/author_names.txt', type=edge, index_attrs=['name'], key_sizes=[26])
    i = 1
    for row in rows:
        el = author_name(id=row[0], name=row[1])
        db.put(el.get_key(), el)
        i += 1
        if i % 1000 == 0:
            print("i=", i)
    print("storing records is done, b_index started..")
    db.b_index()
    db.save()
    print("database created and saved")