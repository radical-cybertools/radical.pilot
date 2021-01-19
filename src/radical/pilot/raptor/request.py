
import time

import radical.utils as ru


# ------------------------------------------------------------------------------
#
class Request(object):

    # poor man's future
    # TODO: use proper future implementation


    # --------------------------------------------------------------------------
    #
    def __init__(self, req):

        self._work   = req
        self._state  = 'NEW'
        self._result = None

        if 'uid' in req: self._uid = req['uid']
        else           : self._uid = ru.generate_id('request')


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
                'mode'  : self._work['mode'],
                'data'  : self._work['data'],
                'result': self._result}


    # --------------------------------------------------------------------------
    #
    def set_result(self, out, err, ret):
        '''
        This is called by the master to fulfill the future
        '''

        self._result = {'out': out,
                        'err': err,
                        'ret': ret}

        if ret: self._state = 'FAILED'
        else  : self._state = 'DONE'


    # --------------------------------------------------------------------------
    #
    def wait(self):

        while self.state not in ['DONE', 'FAILED']:
            time.sleep(0.1)

        return self._result


# ------------------------------------------------------------------------------

