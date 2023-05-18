#!/usr/bin/env python

import pymongo
import threading

DB_NAME         = 'test'
COLLECTION_NAME = 'events'


# ------------------------------------------------------------------------------
#
class Subscriber(threading.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__(self, num):

        super(Subscriber, self).__init__()
        self.conn  = pymongo.MongoClient()
        self.db    = self.conn[DB_NAME]
        self.col   = self.db[COLLECTION_NAME]
        self._stop = threading.Event()
        self.num   = num


    # --------------------------------------------------------------------------
    #
    def stop(self):
        self._stop.set()


    # --------------------------------------------------------------------------
    #
    def run(self):

        cursor = self.col.find(tailable=True, await_data=True)

        while cursor.alive and not self._stop.isSet():
            try:
                record = cursor.next()
                print "< %s" % record
            except StopIteration:
                print "? %s" % self.num
            else:
                print "=  %s: %s" % (self.num, record)



# ------------------------------------------------------------------------------
#
class Publisher(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        self.conn = pymongo.MongoClient()
        self.db   = self.conn[DB_NAME]
        self.col  = self.db[COLLECTION_NAME]

    # --------------------------------------------------------------------------
    #
    def insert(self, data):

        self.col.insert({'item': data})
        self.col.insert({'item': data})
        self.col.insert({'item': data})


# ------------------------------------------------------------------------------
#
def main():

    conn = pymongo.MongoClient()
    db   = conn[DB_NAME]
  # db.drop_collection(COLLECTION_NAME)
    try:
        db.create_collection(COLLECTION_NAME, capped=True, size=100000)
    except pymongo.errors.CollectionInvalid as e:
        print 'oops'
        return

    pub = Publisher()
    pub.insert('initial')

    threads = []
    for i in xrange(2):
        t = Subscriber(i)
        threads.append(t)

    for t in threads:
        t.start()

    while True:
        x = raw_input('What to insert? (q,quit)')
        
        if x in ['q', 'quit']:
            break

        pub.insert(x)

    for t in threads:
        t.stop()

    for t in threads:
        t.join()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    main()

