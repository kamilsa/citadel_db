from database.btrees import *
from database.hash_db import hash_db
from database.page import page
from relations.student import student
import hashlib
import pickle

__author__ = 'kamil'


def get_dataset():
    filename = 'to_put.txt'
    f = open(filename, 'r')
    line = f.readline()
    studs = []
    while line:
        toks = line.split('$')
        s = student(int(toks[0]), toks[1], toks[3].strip(), toks[2])
        studs.append(s)
        line = f.readline()
    f.close()
    return studs

def get_shuffled_dataset():
    filename = 'rand_to_put.txt'
    f = open(filename, 'r')
    line = f.readline()
    studs = []
    while line:
        toks = line.split('$')
        s = student(int(toks[0]), toks[1], toks[3].strip(), toks[2])
        studs.append(s)
        line = f.readline()
    f.close()
    return studs

def generate_random_dataset():
    lines = []
    f = open('to_put.txt', 'r')
    line = f.readline()
    while line:
        lines.append(line)
        line = f.readline()
    f.close()
    random.shuffle(lines)
    filename = 'rand_to_put.txt'
    f = open(filename, 'w')
    f.writelines(lines)
    f.close()

def page_test():
    # studs = get_dataset()
    # p = page()
    # for stud in studs[0:4]:
    # p.insert(stud)
    # p.store('student.txt', 0)
    p = page(filename='student.txt', page_offset=0)
    print(p.items())
    print(p.get('1', 1))

def tree_test():
    btree = BPlusTree(4)
    for i in range(1000):
        btree.insert(i, i)

def db_test():
    open('student.txt', 'w').close()
    db = hash_db(filename='student.txt', type=student)
    studs = get_shuffled_dataset()
    i = 0
    for stud in studs:
        db.put(stud.get_key(), stud)
        i += 1
        print('#', i)
    pickle.dump(db, open('db_meta.pickle', 'wb'))


    db = pickle.load(open('db_meta.pickle', 'rb'))
    count = 0
    for i in range(1, db.size):
        stud = student(to_parse=db.get(str(i),1))
        # print(stud.get_key())
        if stud.get_key() != str(i):
            count += 1
    print(count)

def my_hash(a_str):
    a_str = str(a_str)
    a_str = a_str.encode('utf8')
    return int(hashlib.md5(a_str).hexdigest(), 16)

# min = 100000000
# for i in range(9000,10000):
#     l = len(bin(my_hash(i)))
#     if l < min:
#         min = l
# print(min)
db_test()