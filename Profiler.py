import time

class Profiler(object):
    counter = 0
    def __enter__(self):
        self._startTime = time.time()
         
    def __exit__(self, type, value, traceback):
        f = open('times.txt','a')
        f.write(str(Profiler.counter) + ';' + str(time.time() - self._startTime) + '\n')
        f.close()
        Profiler.counter += 1
        # print ("Elapsed time: {:.9f} sec".format(time.time() - self._startTime))