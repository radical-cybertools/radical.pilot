
__copyright__ = 'Copyright 2017, http://radical.rutgers.edu'
__license__   = 'MIT'


from .noop import Noop


# ------------------------------------------------------------------------------
#
class Flux(Noop):
    '''
    Pass all scheduling and execution control to the Flux executor
    '''

    # --------------------------------------------------------------------------
    #
    def schedule_task(self, task):

        # this abstract method is not used in this implementation
        assert False


    # --------------------------------------------------------------------------
    #
    def unschedule_task(self, task):

        # this abstract method is ignored in this implementation
        pass


# ------------------------------------------------------------------------------

