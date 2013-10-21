
# from pilot  import *
# from bigjob import *

def get_bj_pilot_api () :
    return __import__ ('pilot')

def get_bj_api () :
    return __import__ ('bigjob')

