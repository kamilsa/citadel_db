from collections import deque
from database.ipage import ipage


class cursor:
    __name__ = 'scan cursor'

    def __init__(self, db=None, filename=None, on_field=None):
        self.type_attrs = db.type.__attrs__
        self.iter = 0  # to iterate items
        self.size = db.size  # number of items in db
        self.items = set()
        self.on_field = on_field
        self.db = db
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
            return tuple(attrs[k] for k in self.type.__attrs__)
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
            return tuple(ent.attrs[k] for k in self.type.__attrs__)

    def has_next(self):
        if self.on_field is None:
            if self.iter == self.size:
                self.refresh()
                return False
            else:
                return True
        else:
            res = self.tree_cursor.next()
            self.tree_cursor.prev()
            if res and len(self.cur_set) == 0:
                return True
            else:
                self.refresh()
                return False

    def refresh(self):
        if self.on_field is None:
            page_offsets = set()
            for p in self.db.pp:
                page_offsets.add(p)
            self.iter = 0
            self.curr_iter = 0  # to iterate within items from page
            self.curr_items = None
            self.do_next_page = True
            self.page_offsets = page_offsets
        else:
            tree = self.db.trees[self.on_field]
            self.tree_cursor = tree.cursor()
            self.iter = 0
            self.curr_iter = 0
            self.do_next_set = True
            self.cur_set = set()


class select_cursor(cursor):
    __name__ = 'select_cursor'
    __lastkey = None  # last visited key

    def __init__(self, db=None, filename=None, on_field=None, greater_than=None, less_than=None, on_cursor=None):
        if db is not None:
            self.type_attrs = db.type.__attrs__  # list of attributes' names
        else:
            self.type_attrs = on_cursor.type_attrs

        self.iter = 0  # to iterate items
        self.items = set()
        self.on_field = on_field
        self.on_cursor = on_cursor
        self.db = db
        if db is not None and on_cursor is None:  # so, we actually do scan
            self.size = db.size  # number of items in db
            self.type = db.type
            if on_field is None:  # do scan on key field
                print "Something tried to create range-query cursor without assigned field"
            else:  # do scan on given field(it should have b_index on it)
                tree = db.trees[on_field]
                self.tree_cursor = tree.cursor()
                # define start and end of cursor
                self.less_than = less_than
                self.greater_than = greater_than
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
        elif db is None and on_cursor is not None:  # if build cursor based on other cursor
            self.on_cursor = on_cursor
            self.size = on_cursor.size  # number of items in db
            self.type = on_cursor.type
            if on_field is None:  # do scan on key field
                print "Something tried to create range-query cursor without assigned field"
            else:  # do scan on given field(it should have b_index on it)
                tree = self.on_cursor.db.trees[on_field]
                self.on_cursor.tree_cursor = tree.cursor()
                # define start and end of cursor
                if less_than is None:
                    self.less_than = "ZZZZZZZZZZZZZZZZZZZ"
                else:
                    self.less_than = less_than
                if greater_than is None:
                    self.greater_than = "00000000000"
                else:
                    self.greater_than = greater_than
                if greater_than is not None:
                    tree[greater_than] = "NoneString"
                    tree.commit()
                    self.on_cursor.tree_cursor.position(greater_than)
                    self.on_cursor.tree_cursor.next()
                    key = self.on_cursor.tree_cursor.read_key()
                    del tree[greater_than]
                    tree.commit()
                    self.on_cursor.tree_cursor = tree.cursor(key)

                self.on_cursor.curr_iter = 0
                self.on_cursor.do_next_set = True
                self.on_cursor.cur_set = set()
        else:
            "Something went wrong in constructor of select cursor"

    def next(self):
        if self.on_cursor is None:  # so, we actually do scan
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
                return tuple(ent.attrs[k] for k in self.type.__attrs__)
        elif self.on_cursor is not None:
            if self.on_cursor.on_field is None:  # so, we iterate using hash-index
                res = None
                on_field_id = self.type.__attrs__.index(self.on_field)
                while True:
                    res = self.on_cursor.next()
                    if self.less_than > res[on_field_id] > self.greater_than:
                        break
                return res

            else:  # so, we iterate using b_tree index on field "on_field"
                return self.on_cursor.next()
        else:
            print "something went wrong in next in select cursor"

    def has_next(self):
        if self.on_cursor is None:
            if self.on_field is None:
                print "Something tried to create range-query cursor without assigned field"
            else:
                res = self.tree_cursor.next()
                key = self.tree_cursor.read_key()
                self.tree_cursor.prev()
                if key > self.less_than:
                    self.refresh()
                    return False
                if res and len(self.cur_set) == 0:
                    return True
                else:
                    self.refresh()
                    return False
        else:
            if self.on_cursor.on_field is not None:
                if not self.on_cursor.tree_cursor.next():
                    return False
                key = self.on_cursor.tree_cursor.read_key()
                if key > self.less_than:
                    return False
                self.on_cursor.tree_cursor.prev()
                return self.on_cursor.has_next()
            else:
                tmp_iter = self.on_cursor.iter
                tmp_curr_iter = self.on_cursor.curr_iter
                tmp_page_offsets = self.on_cursor.page_offsets.copy()
                tmp_do_next_set = self.on_cursor.do_next_set
                items = self.on_cursor.curr_items
                while True:
                    if tmp_iter == self.on_cursor.size:
                        self.refresh()
                        return False
                    if len(tmp_page_offsets) == 0:
                        self.refresh()
                        return False
                    if tmp_do_next_set:
                        curr_page = ipage(page_offset=tmp_page_offsets.pop(), filename=self.on_cursor.filename)
                        items = curr_page.items()
                        tmp_do_next_set = False
                        tmp_curr_iter = 0
                    item = items[tmp_curr_iter]
                    tmp_curr_iter += 1
                    if tmp_curr_iter == len(items):
                        tmp_do_next_set = True
                        tmp_curr_iter = 0
                    tmp_iter += 1
                    attrs = self.on_cursor.type(to_parse=item).attrs
                    if self.less_than > attrs[self.on_field] > self.greater_than:
                        return True

    def refresh(self):
        if self.db is not None and self.on_cursor is None:
            tree = self.db.trees[self.on_field]
            self.tree_cursor = tree.cursor()
            # define start and end of cursor
            if self.greater_than is not None:
                tree[self.greater_than] = "NoneString"
                tree.commit()
                self.tree_cursor.position(self.greater_than)
                self.tree_cursor.next()
                key = self.tree_cursor.read_key()
                del tree[self.greater_than]
                tree.commit()
                self.tree_cursor = tree.cursor(key)

            self.curr_iter = 0
            self.do_next_set = True
            self.cur_set = set()
        elif self.db is None and self.on_cursor is not None:
            if self.on_field is None:  # do scan on key field
                print "Something tried to create range-query cursor without assigned field"
            else:  # do scan on given field(it should have b_index on it)
                tree = self.on_cursor.db.trees[self.on_field]
                self.on_cursor.refresh()
                self.on_cursor.tree_cursor = tree.cursor()
                # define start and end of cursor
                if self.greater_than is not None:
                    tree[self.greater_than] = "NoneString"
                    tree.commit()
                    self.on_cursor.tree_cursor.position(self.greater_than)
                    self.on_cursor.tree_cursor.next()
                    key = self.on_cursor.tree_cursor.read_key()
                    del tree[self.greater_than]
                    tree.commit()
                    self.on_cursor.tree_cursor = tree.cursor(key)

                self.on_cursor.curr_iter = 0
                self.on_cursor.do_next_set = True
                self.on_cursor.cur_set = set()


class project_cursor(cursor):
    __name__ = 'project_cursor'

    def __init__(self, db=None, filename=None, fields=None, ordered_on=None, on_cursor=None):
        if db is not None:
            self.type_attrs = db.type.__attrs__  # list of attributes' names
            self.size = db.size  # number of items in db
        else:
            self.type_attrs = on_cursor.type.__attrs__
            self.size = on_cursor.size
        self.fields = fields
        self.iter = 0  # to iterate items
        self.ordered_on = ordered_on
        self.db = db
        self.on_cursor = on_cursor

        # remove redundant attributes from type_attrs
        for attr in self.type_attrs:
            if attr not in fields:
                self.type_attrs.remove(attr)

        if db is not None and on_cursor is None:  # so, we actually do scan
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
        elif db is None and on_cursor is not None:
            self.type = on_cursor.type

    def next(self):
        if self.on_cursor is None:
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
                return tuple(res[k] for k in self.fields)
        else:
            res = self.on_cursor.next()
            tmp = []

            # remove redundant attributes from result of calling next on given cursor
            for attr in self.on_cursor.type_attrs:
                if attr in self.type_attrs:
                    tmp.append(self.on_cursor.type_attrs.index(attr))
            return tuple(res[t] for t in tmp)

    def has_next(self):
        if self.on_cursor is None:
            if self.ordered_on is None:
                if self.iter == self.size:
                    self.refresh()
                    return False
                else:
                    return True
            else:
                res = self.tree_cursor.next()
                self.tree_cursor.prev()
                if res and len(self.cur_set) == 0:
                    return True
                else:
                    self.refresh()
                    return False
        else:
            return self.on_cursor.has_next()

    def refresh(self):
        if self.on_cursor is None:
            if self.ordered_on is None:
                page_offsets = set()
                for p in self.db.pp:
                    page_offsets.add(p)
                self.iter = 0
                self.curr_iter = 0  # to iterate within items from page
                self.curr_items = None
                self.do_next_page = True
                self.page_offsets = page_offsets
            else:
                tree = self.db.trees[self.ordered_on]
                self.tree_cursor = tree.cursor()
                self.iter = 0
                self.curr_iter = 0
                self.do_next_set = True
                self.cur_set = set()
        else:
            self.on_cursor.refresh()
