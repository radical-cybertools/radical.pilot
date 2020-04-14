
import time

import radical.utils     as ru


# ------------------------------------------------------------------------------
#
class Request(object):

    # poor man's future
    # TODO: use proper future implementation


    # --------------------------------------------------------------------------
    #
    def __init__(self, req):

        self._uid    = ru.generate_id('request')
        self._work   = req
        self._state  = 'NEW'
        self._result = None


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    @property
    def state(self):
        return self._state


    @property
    def work(self):
        return self._work


    @property
    def result(self):
        return self._result


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        '''
        produce the request message to be sent over the wire to the workers
        '''

        return {'uid'   : self._uid,
                'state' : self._state,
                'result': self._result,
                'call'  : self._work['call'],
                'args'  : self._work.get('args',   []),
                'kwargs': self._work.get('kwargs', {}),
               }


    # --------------------------------------------------------------------------
    #
    def set_result(self, result, error):
        '''
        This is called by the master to fulfill the future
        '''

        self._result = result
        self._error  = error

        if error: self._state = 'FAILED'
        else    : self._state = 'DONE'


    # --------------------------------------------------------------------------
    #
    def wait(self):

        while self.state not in ['DONE', 'FAILED']:
            time.sleep(0.1)

        return self._result


# ------------------------------------------------------------------------------

