import faker
import names
from database.btrees import *
from database.hash_db import hash_db
from database.page import page
from relations.student import student
import hashlib
import pickle
from Profiler import Profiler
import numpy as np
import matplotlib.pyplot as plt
# from sklearn.linear_model import LinearRegression

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
    # studs = get_dataset()
    # p = page()
    # for stud in studs[0:10]:
    #     p.insert(stud)
    #     p.store('page.txt', 0)
    p = page(filename='student.txt', page_offset=0)
    for item in p.items():
        stud = student(to_parse=item)
        print(stud.attrs)


def tree_test():
    btree = BPlusTree(4)
    for i in range(1000):
        btree.insert(i, i)


def db_test():
    open('student.txt', 'w').close()
    db = hash_db(filename='student.txt', type=student)
    # studs = get_shuffled_dataset()
    studs = get_shuffled_million()
    i = 0
    for stud in studs:
        with Profiler() as p:
            db.put(stud.get_key(), stud)
            i += 1
            print('#', i)
    pickle.dump(db, open('db_meta.pickle', 'wb'))


    # db = pickle.load(open('db_meta.pickle', 'rb'))
    # db.remove('2', 1)


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
    # lr.fit(xx,yy)

    # x = np.linspace(0, 2 * np.pi, 50)
    # y = np.sin(x)
    # x = xx[::500]
    # y = lr.predict(x)
    # y2 = y + 0.1 * np.random.normal(size=x.shape)

    fig, ax = plt.subplots()
    # ax.plot(x, y, 'k--')
    # ax.plot(x, y2, 'ro')
    ax.plot(xx, yy, 'ro', markersize = 1)


    # set ticks and tick labels
    # ax.set_xlim((0, 2 * np.pi))
    ax.set_xlim((0, len(xx)))
    ax.set_xticks(xx[::20000])
    ax.set_xticklabels(xx[::20000])
    # ax.set_ylim((-1.5, 1.5))
    ax.set_ylim((0, max_y))
    ax.set_yticks([0, max_y/3, 2*max_y/3, max_y])
    ax.set_yticklabels([0, max_y/3, 2*max_y/3, max_y])

    # Only draw spine between the y-ticks
    ax.spines['left'].set_bounds(-1, 1)
    # Hide the right and top spines
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    # Only show ticks on the left and bottom spines
    ax.yaxis.set_ticks_position('left')
    ax.xaxis.set_ticks_position('bottom')

    plt.show()


# generate_random_million()
plot()
# generate_million()
# db_test()
# page_test()
