
__copyright__ = 'Copyright 2017, http://radical.rutgers.edu'
__license__   = 'MIT'


from .noop import Noop


# ------------------------------------------------------------------------------
#
class Flux(Noop):
    '''
    Pass all scheduling and execution control to the Flux executor
    '''
    pass


# ------------------------------------------------------------------------------

