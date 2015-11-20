# import faker
# import names
import os
from database.btrees import *
from database.cursor import select_cursor, project_cursor
from database.hash_db import hash_db
from database.page import page
from database.ipage import ipage
from relations.pair import pair
from relations.student import student
import hashlib
import pickle
from Profiler import Profiler
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import numpy as np

__author__ = 'kamil'

def get_random_student(id):
    fake = faker.Faker()
    name = names.get_full_name()
    first_last = name.lower().split(' ')
    email = first_last[0][0] + '.' + first_last[1] + '@innopolis.ru'
    address = fake.address().replace('\n', ' ')
    return student(id, name, email, address)

def generate_million():
    filename = 'million.txt'
    f = open(filename, 'w')
    for i in range(1, 1000000):
        if i % 100 == 0:
            print('#', i)

        stud = get_random_student(i)
        to_ins = str(stud.attrs['id^']) + '$' + stud.attrs['name'] + '$' + stud.attrs['address'] + '$' + stud.attrs[
            'email'] + '\n'
        f.write(to_ins)
    f.close()

def generate_random_million():
    lines = []
    f = open('million.txt', 'r')
    line = f.readline()
    while line:
        lines.append(line)
        line = f.readline()
    f.close()
    random.shuffle(lines)
    filename = 'rand_million.txt'
    f = open(filename, 'w')
    f.writelines(lines)
    f.close()

def get_shuffled_million():
    filename = 'rand_million.txt'
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
    studs = get_dataset()
    p = ipage()
    open('page.txt', 'w').close()
    from mx.BeeBase import BeeDict
    tree = BeeDict.BeeStringDict(os.getcwd() + '/storage/' + student.__name__ + 'name',
                                                 keysize=256)

    for stud in studs[0:10]:
        p.insert(stud)
        p.store('page.txt', 0)
    p.store_to_tree(tree, student, 'name', 'page.txt')
    print (zip(tree.keys(), tree.values()))
    tree.close()

    # p = page(filename='student.txt', page_offset=0)
    # for item in p.items():
    #     stud = student(to_parse=item)
    #     print(stud.attrs)

def tree_test():
    from mx.BeeBase import BeeDict
    # Here is a very simple file based string dictionary:
    # d = BeeDict.BeeStringDict('storage/BeeStringDict.example', keysize=26)
    # studs = get_shuffled_million()
    # i = 0
    # max = ''
    # for stud in studs[:100000]:
    #     with Profiler() as p:
    #         d[stud.attrs['name']] = stud.get_string()
    #         i += 1
    #         d.commit()
    #     if i % 5000 == 0:
    #         print('#', i)
    # d.close()

    d = BeeDict.BeeStringDict('storage/BeeStringDict.example', keysize=26)
    martha = d.cursor(key='Martha Morrow')
    print(martha.next())
    # print(len(d))
    # print(d['Martha Morrow'])
    print(martha.key, d[martha.key])

def db_test():
    print 'create database with capacity 100000 test is started..\n'
    open(os.getcwd() + '/storage/student.txt', 'wb').close()
    open(os.getcwd() + '/times.txt', 'wb').close()
    db = hash_db(filename=os.getcwd() + '/storage/student.txt', type=student, index_attrs=['name'], key_sizes=[26])
    # studs = get_shuffled_dataset()

    print('loading dataset')
    studs = get_shuffled_million()
    i = 0
    for stud in studs[:10000]:
        with Profiler() as p:
            db.put(stud.get_key(), stud)
            i += 1
            if i % 50 == 0:
                print('#', i)
    db.b_index()
    for index in db.index_attrs:
        tree = db.trees[index]
    # print(db.get('581200', 1))
    # print(db.trees['name']['Matthew Cervantes'])
    db.save()

def db_pair_test():
    print 'create database with capacity 100000 test is started..\n'
    open(os.getcwd() + '/storage/student.txt', 'wb').close()
    db = hash_db(filename=os.getcwd() + '/storage/student.txt', type=student, index_attrs=['name'], key_sizes=[26])
    # studs = get_shuffled_dataset()

    print('loading dataset')
    pairs = []
    size = 1000
    for i in range(size):
        p = pair(id = i, id1 = i, id2 = i)
        pairs.append(p)
    i = 0
    for p in pairs:
        with Profiler() as p:
            db.put(p.get_key(), p)
            i += 1
            if i % 50 == 0:
                print('#', i)
    # print(db.get('581200', 1))
    # print(db.trees['name']['Matthew Cervantes'])
    db.save()

def page_pair_test():
    pairs = []
    size = 10000
    for i in range(size):
        p = pair(id = i, id1 = i, id2 = i)
        pairs.append(p)
    p = ipage()
    for pa in pairs:
        if not p.is_fit(pa):
            break
        p.insert(pa)
        p.store('page.txt', 0)
    print(p.get("100", 1))

def db_load_test():
    print 'load test is started..\n'
    db = hash_db(type=student, from_dump=True)
    # print student(db.get('581200', 1)).attrs
    print 'test get student with id = 581200..\n'
    print(student(to_parse=db.get('581200', 1)))
    print 'test neighbours of Matthew Cervantes..\n'
    nbrs = db.neighbours('name', 'Matthew Cervantes', 10)
    for nbr in nbrs:
        print nbr.attrs
    print 'test update Matthew Cervantes to Kamil..\n'
    print(student(to_parse=db.get('581200', 1)).attrs)
    db.update('581200', 1, {'name': 'Kamil'})
    print(student(to_parse=db.get('581200', 1)).attrs)
    print 'test remove Kamil by his id..\n'
    db.remove('581200', 1)
    if db.get('581200', 1) == None:
        print('Kamil is removed!')

def cursor_test():
    from database.cursor import cursor

    db = hash_db(type=student, from_dump=True)
    # c = cursor(db=db, filename=db.filename)
    # c = select_cursor(db=db,filename=db.filename, on_field='name', greater_than=None, less_than="B")
    c = project_cursor(db=db,filename=db.filename, fields={'name', 'email'}, ordered_on='name')
    while c.has_next():
        print c.next()

def my_hash(a_str):
    a_str = str(a_str)
    a_str = a_str.encode('utf8')
    return int(hashlib.md5(a_str).hexdigest(), 16)


def plot():
    f = open('times.txt', 'r')
    xx = []
    yy = []
    line = f.readline()
    max_y = -1
    while line:
        toks = line.split(';')
        xx.append(int(toks[0]))
        fl = float(toks[1].strip())
        if fl > max_y:
            max_y = fl
        yy.append(fl)
        line = f.readline()
    f.close()

    # lr = LinearRegression()

    # np_xx = np.array(xx[::10])
    # np_yy = np.array(yy[::10])
    # lr.fit(np_xx.reshape(1,len(np_xx)) ,np_yy.reshape(1, len(np_yy)))

    # print lr.coef_
    # exit()

    # x = np.linspace(0, 2 * np.pi, 50)
    # y = np.sin(x)
    # x = np_xx
    # y = lr.predict(x)
    # y2 = y + 0.1 * np.random.normal(size=x.shape)

    fig, ax = plt.subplots()
    # ax.plot(x, y[0], 'r-')
    # ax.plot(x, y2, 'ro')
    ax.plot(xx[::100], yy[::100], 'ro', markersize=1)


    # set ticks and tick labels
    ax.set_xlim((0, 2 * np.pi))
    max_y = 0.0264
    ax.set_xlim((0, len(xx[:100000])))
    ax.set_xticks(xx[:100000:20000])
    ax.set_xticklabels(xx[::20000])
    # ax.set_ylim((-1.5, 1.5))
    ax.set_ylim((0, max_y))
    ax.set_yticks([0, max_y / 3, 2 * max_y / 3, max_y])
    ax.set_yticklabels([0, max_y / 3, 2 * max_y / 3, max_y])

    # Only draw spine between the y-ticks
    ax.spines['left'].set_bounds(-1, 1)
    # Hide the right and top spines
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    # Only show ticks on the left and bottom spines
    ax.yaxis.set_ticks_position('left')
    ax.xaxis.set_ticks_position('bottom')

    plt.show()


def task_b_tree():
    a = 'kamilSALAKHIEV'
    tree = BPlusTree(3)
    a = [53, 57, 59, 73, 95, 51, 55, 64, 75, 74, 63, 61, 58, 69, 54, 81, 85, 106, 110, 99, 77, 114, 23]
    b = [56,101,91,92,93,96,94,90,98,100,20,10,40]
    a.sort()
    # tree.insert(56)
    for i in b:
        tree[i] = i
    for i in b:
        tree[i] = i
    for i in 'kamilSALAKHIEV':
        tree[ord(i)] = ord(i)
    #
    print(tree)

# generate_random_million()
# plot()
# generate_million()
# db_test()
# db_load_test()
# page_test()
# page_pair_test()
# tree_test()
cursor_test()
