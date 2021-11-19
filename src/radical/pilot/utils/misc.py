
import os
import time
import builtins

from typing import Any, Union, Optional

import radical.utils as ru

# max number of t out/err chars to push to tail
MAX_IO_LOGLENGTH = 1024


# ------------------------------------------------------------------------------
#
def tail(txt: str, maxlen: int = MAX_IO_LOGLENGTH) -> str:

    # shorten the given string to the last <n> characters, and prepend
    # a notification.  This is used to keep logging information in mongodb
    # manageable(the size of mongodb documents is limited).

    if not txt:
        return ''

    if len(txt) > maxlen:
        return "[... CONTENT SHORTENED ...]\n%s" % txt[-maxlen:]
    else:
        return txt


# ------------------------------------------------------------------------------
#
def get_rusage() -> str:

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
def create_tar(tgt: str, dnames: str) -> None:
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


# ------------------------------------------------------------------------------
#
def get_type(type_name: str) -> type:
    '''
    get a type object from a type name (str)
    '''

    ret = getattr(builtins, type_name, None)

    if ret:
        return ret

    obj = globals().get(type_name)

    if not obj:
        return None

    if not isinstance(obj, type):
        return None

    return repr(obj)


# ------------------------------------------------------------------------------
#
def load_class(fpath: str,
               cname: str,
               ctype: Optional[Union[type,str]] = None) -> Optional[Any]:
    '''
    load class `cname` from a source file at location `fpath`
    and return it (the class, not an instance).
    '''

    from importlib import util as imp

    pname  = os.path.splitext(os.path.basename(fpath))[0]
    spec   = imp.spec_from_file_location(pname, fpath)
    plugin = imp.module_from_spec(spec)

    spec.loader.exec_module(plugin)

    ret = getattr(plugin, cname)

    if ctype:

        if isinstance(ctype, str):
            ctype_name = ctype
            ctype = get_type(ctype_name)

            if not ctype:
                raise ValueError('cannot type check %s' % ctype_name)

        if not issubclass(ret, ctype):
            return None

    return ret


# ------------------------------------------------------------------------------

