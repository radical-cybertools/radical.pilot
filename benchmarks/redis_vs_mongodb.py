#!/usr/bin/env python

# from http://stackoverflow.com/questions/5252577/how-much-faster-is-redis-than-mongodb/5870726#5870726

import sys, time
from pymongo import Connection
import redis



# connect to redis & mongodb
redis = redis.Redis(host='localhost')
mongo = Connection(host='localhost').test
collection = mongo['test']
collection.ensure_index('key', unique=True)

def mongo_set(data):
    for k, v in data.iteritems():
        collection.insert({'key': k, 'value': v})

def mongo_get(data):
    for k in data.iterkeys():
        val = collection.find_one({'key': k}, fields=('value',)).get('value')

def redis_set(data):
    for k, v in data.iteritems():
        redis.set(k, v)

def redis_get(data):
    for k in data.iterkeys():
        val = redis.get(k)

def do_tests(num, tests):
    # setup dict with key/values to retrieve
    data = {'key' + str(i): 'val' + str(i)*100 for i in range(num)}
    # run tests
    for test in tests:
        start = time.time()
        test(data)
        elapsed = time.time() - start
        print "Completed %s: %d ops in %.2f seconds : %.1f ops/sec" % (test.__name__, num, elapsed, num / elapsed)

if __name__ == '__main__':
    num = 1000 if len(sys.argv) == 1 else int(sys.argv[1])
    tests = [mongo_set, mongo_get, redis_set, redis_get] # order of tests is significant here!
    do_tests(num, tests)

