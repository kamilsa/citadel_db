__author__ = 'bulat'

import os
import tarfile


def _zip_dir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            if not file.__contains__('locked') and not file.__contains__('.DS_Store'):
                print("Writing " + file)
                # ziph.write(os.path.join(root, file))
                ziph.add(os.path.join(root, file))


class Storage:
    db_name = 'main.dat'
    db_storage = None
    table_names = None
    current_file = None

    def __init__(self):
        # initialy will open for read
        # self.db_storage = zipfile.ZipFile(self.db_name, 'r')

        # self.db_storage = BZ2File(self.db_name)
        # print self.db_storage.
        # self.table_names = self.db_storage.namelist()
        self.db_storage = tarfile.open(self.db_name, 'r:')
        if not self.db_storage is None:
            self.table_names = self.db_storage.getnames()


    def open_file(self, file_name):
        if file_name not in self.table_names:
            raise (BaseException("No file in database " + file_name))
        self.db_storage.close()
        self.db_storage = tarfile.open(self.db_name, 'r:')
        self.current_file = self.db_storage.extractfile(file_name)
        self.db_storage.members = []

    def close(self):
        self.db_storage.members = []
        self.db_storage.close()


    def read(self, n):
        if self.current_file is None:
            raise (BaseException("Open specific file first"))
        print self.current_file.read(n)

    def write(self):
        pass

    def seek(self, offset):
        if self.current_file is None:
            raise (BaseException("Open specific file first"))
        self.current_file.seek(offset)

    def save_all(self, path):
        # self.db_storage.close()
        # self.db_storage = zipfile.ZipFile(self.db_name, 'w')
        # self.db_storage = GzipFile(self.db_name, 'w:')
        print("Saving all data")
        self.db_storage = tarfile.open(self.db_name, 'w:')
        _zip_dir(path, self.db_storage)
        self.db_storage.close()

    def save(self, path):
        '''
        Save on append. One file
        :param path:
        :return:
        '''
        self.db_storage.close()
        self.db_storage = tarfile.open(self.db_name, 'a')
        self.db_storage.add(path)
        self.db_storage.close()
