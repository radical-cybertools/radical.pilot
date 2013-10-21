
from pilot  import *
from bigjob import *

#   search_paths = sys.path[:]
#
#   if not predicate:
#       return __import__(name)
#
#   while 1:
#       fp, pathname, desc = imp.find_module(name, search_paths)
#       module = imp.load_module(name, fp, pathname, desc)
#
#       if predicate(module):
#           return module
#       else: 
#           search_paths = search_paths[1:]

def get_bj_pilot_api () :
    return __import__ ('pilot')

def get_bj_api () :
    return __import__ ('bigjob')

