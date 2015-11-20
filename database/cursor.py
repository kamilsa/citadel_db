from collections import deque
from database.ipage import ipage


class cursor:
    def __init__(self, db=None, filename=None, on_field=None):
        self.iter = 0  # to iterate items
        self.size = db.size  # number of items in db
        self.items = set()
        self.q = deque()
        self.on_field = on_field
        if db is not None:  # so, we actually do scan
            self.type = db.type
            if on_field is None:  # do scan on key field
                page_offsets = set()
                self.filename = db.filename
                for p in db.pp:
                    page_offsets.add(p)
                self.curr_iter = 0  # to iterate within items from page
                self.curr_items = None
                self.do_next_page = True
                self.page_offsets = page_offsets
                # for po in page_offsets:
                #     p = ipage(page_offset=po, filename=filename)
                #     for item in p.items():
                #         self.q.append(item)
            else:  # do scan on given field(it should have b_index on it)
                tree = db.trees[on_field]
                self.tree_cursor = tree.cursor()
                self.curr_iter = 0
                self.do_next_set = True
                self.cur_set = set()

    def next(self):
        if self.on_field is None:  # so, we iterate using hash-index
            if self.do_next_page:
                curr_page = ipage(page_offset=self.page_offsets.pop(), filename=self.filename)
                self.curr_items = curr_page.items()
                self.do_next_page = False
            item = self.curr_items[self.curr_iter]
            self.curr_iter += 1
            if self.curr_iter == len(self.curr_items):
                self.do_next_page = True
                self.curr_iter = 0
            self.iter += 1
            attrs = self.type(to_parse=item).attrs
            tuple(attrs[k] for k in attrs)
            return self.type(to_parse=item).attrs
            # return tuple(attrs[k] for k in attrs)
        else:  # so, we iterate using b_tree index on field "on_field"
            if self.do_next_set:
                self.tree_cursor.next()
                self.cur_set = self.tree_cursor.read_value().copy()
                self.do_next_set = False
            res = self.cur_set.pop()
            if len(self.cur_set) == 0:
                self.do_next_set = True
            toks = res.split(',')
            f = open(toks[0], 'r')
            f.seek(int(toks[1]))
            res = f.read(int(toks[2]))
            f.close()
            ent = self.type(to_parse=res)
            return ent.attrs

    def has_next(self):
        if self.on_field is None:
            if self.iter == self.size:
                return False
            else:
                return True
        else:
            res = self.tree_cursor.next()
            self.tree_cursor.prev()
            if res and len(self.cur_set) == 0:
                return True
            else:
                return False


class select_cursor(cursor):
    def __init__(self, db=None, filename=None, on_field=None, greater_than=None, less_than=None):
        self.iter = 0  # to iterate items
        self.size = db.size  # number of items in db
        self.items = set()
        self.q = deque()
        self.on_field = on_field
        if db is not None:  # so, we actually do scan
            self.type = db.type
            if on_field is None:  # do scan on key field
                print "Something tried to create range-query cursor without assigned field"
            else:  # do scan on given field(it should have b_index on it)
                tree = db.trees[on_field]
                self.tree_cursor = tree.cursor()
                # define start and end of cursor
                self.less_than = less_than
                if greater_than is not None:
                    tree[greater_than] = "NoneString"
                    tree.commit()
                    self.tree_cursor.position(greater_than)
                    self.tree_cursor.next()
                    key = self.tree_cursor.read_key()
                    del tree[greater_than]
                    tree.commit()
                    self.tree_cursor = tree.cursor(key)

                self.curr_iter = 0
                self.do_next_set = True
                self.cur_set = set()

    def next(self):
        if self.on_field is None:  # so, we iterate using hash-index
            print "Something tried to create range-query cursor without assigned field"
        else:  # so, we iterate using b_tree index on field "on_field"
            if self.do_next_set:
                self.tree_cursor.next()
                self.cur_set = self.tree_cursor.read_value().copy()
                self.do_next_set = False
            res = self.cur_set.pop()
            if len(self.cur_set) == 0:
                self.do_next_set = True
            toks = res.split(',')
            f = open(toks[0], 'r')
            f.seek(int(toks[1]))
            res = f.read(int(toks[2]))
            f.close()
            ent = self.type(to_parse=res)
            return ent.attrs

    def has_next(self):
        if self.on_field is None:
            print "Something tried to create range-query cursor without assigned field"
        else:
            res = self.tree_cursor.next()
            key = self.tree_cursor.read_key()
            self.tree_cursor.prev()
            if key > self.less_than:
                return False
            if res and len(self.cur_set) == 0:
                return True
            else:
                return False


class project_cursor(cursor):
    def __init__(self, db=None, filename=None, fields=None, ordered_on = None):
        self.fields = fields
        self.iter = 0  # to iterate items
        self.size = db.size  # number of items in db
        self.ordered_on = ordered_on
        if db is not None:  # so, we actually do scan
            self.type = db.type
            if ordered_on is None:  # do scan on key field
                page_offsets = set()
                self.filename = db.filename
                for p in db.pp:
                    page_offsets.add(p)
                self.curr_iter = 0  # to iterate within items from page
                self.curr_items = None
                self.do_next_page = True
                self.page_offsets = page_offsets
            else:  # do scan on given field(it should have b_index on it)
                tree = db.trees[ordered_on]
                self.tree_cursor = tree.cursor()
                self.curr_iter = 0
                self.do_next_set = True
                self.cur_set = set()

    def next(self):
        if self.ordered_on is None:  # so, we iterate using hash-index
            if self.do_next_page:
                curr_page = ipage(page_offset=self.page_offsets.pop(), filename=self.filename)
                self.curr_items = curr_page.items()
                self.do_next_page = False
            item = self.curr_items[self.curr_iter]
            self.curr_iter += 1
            if self.curr_iter == len(self.curr_items):
                self.do_next_page = True
                self.curr_iter = 0
            self.iter += 1
            res = {}
            attrs = self.type(to_parse=item).attrs
            for field in self.fields:
                res[field] = attrs[field]
            return res
        else:  # so, we iterate using b_tree index on field "on_field"
            if self.do_next_set:
                self.tree_cursor.next()
                self.cur_set = self.tree_cursor.read_value().copy()
                self.do_next_set = False
            res = self.cur_set.pop()
            if len(self.cur_set) == 0:
                self.do_next_set = True
            toks = res.split(',')
            f = open(toks[0], 'r')
            f.seek(int(toks[1]))
            res = f.read(int(toks[2]))
            f.close()
            ent = self.type(to_parse=res)

            res = {}
            for field in self.fields:
                res[field] = ent.attrs[field]
            return res

    def has_next(self):
        if self.ordered_on is None:
            if self.iter == self.size:
                return False
            else:
                return True
        else:
            res = self.tree_cursor.next()
            self.tree_cursor.prev()
            if res and len(self.cur_set) == 0:
                return True
            else:
                return False