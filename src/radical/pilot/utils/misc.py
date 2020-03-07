
import os
import time
import errno

import radical.utils as ru

# max number of cu out/err chars to push to tail
MAX_IO_LOGLENGTH = 1024


# ------------------------------------------------------------------------------
#
def tail(txt, maxlen=MAX_IO_LOGLENGTH):

    # shorten the given string to the last <n> characters, and prepend
    # a notification.  This is used to keep logging information in mongodb
    # manageable(the size of mongodb documents is limited).

    if not txt:
        return txt

    if len(txt) > maxlen:
        return "[... CONTENT SHORTENED ...]\n%s" % txt[-maxlen:]
    else:
        return txt


# ------------------------------------------------------------------------------
#
def get_rusage():

    import resource

    self_usage  = resource.getrusage(resource.RUSAGE_SELF)
    child_usage = resource.getrusage(resource.RUSAGE_CHILDREN)

    rtime = time.time()
    utime = self_usage.ru_utime  + child_usage.ru_utime
    stime = self_usage.ru_stime  + child_usage.ru_stime
    rss   = self_usage.ru_maxrss + child_usage.ru_maxrss

    return "real %3f sec | user %.3f sec | system %.3f sec | mem %.2f kB" \
         % (rtime, utime, stime, rss)


# ------------------------------------------------------------------------------
#
def rec_makedir(target):

    # recursive makedir which ignores errors if dir already exists

    try:
        os.makedirs(target)

    except OSError as e:
        # ignore failure on existing directory
        if e.errno == errno.EEXIST and os.path.isdir(os.path.dirname(target)):
            pass
        else:
            raise


# ----------------------------------------------------------------------------------
#
def create_tar(tgt, dnames):
    '''
    Create a tarball on the file system which contains all given directories
    '''

    uid   = os.getuid()
    gid   = os.getgid()
    mode  = 16893
    mtime = int(time.time())

    fout  = open(tgt, 'wb')

    def rpad(s, size):
        return s + (size - len(s)) * '\0'

    def write_dir(path):
        data  = rpad(path, 100) \
              + rpad('%o' % mode,   8) \
              + rpad('%o' % uid,    8) \
              + rpad('%o' % gid,    8) \
              + rpad('%o' % 0,     12) \
              + rpad('%o' % mtime, 12) \
              + 8 * '\0' + '5'
        cksum = 256 + sum(ord(h) for h in data)
        data  = rpad(data  , 512)
        data  = data  [:-364] + '%06o\0' % cksum + data[-357:]
        fout.write(ru.as_bytes(data))

    for dname in dnames:
        write_dir(dname)

    fout.close()


# ----------------------------------------------------------------------------------

