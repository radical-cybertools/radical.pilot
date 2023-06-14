

from typing import Optional, Dict, Any

import radical.utils as ru


# ------------------------------------------------------------------------------
#
class HeartbeatMessage(ru.Message):

    # ------------------------------
    class Payload(ru.TypedDict):
        _schema   = {'uid': str  }
        _defaults = {'uid': None }
    # ------------------------------

    _schema = {
        'payload': Payload
    }

    _defaults = {
        'msg_type': 'heartbeat',
        'payload' : {}
    }



    # --------------------------------------------------------------------------
    def __init__(self, uid      : Optional[str]            = None,
                       from_dict: Optional[Dict[str, Any]] = None):
        '''
        support msg construction and usage like this:

            hb_msg = rp.HeartbeatMessage(uid='foo.1')
            assert hb_msg.uid == 'foo.1

        '''

        if uid:
            from_dict = {'payload': {'uid': uid}}

        super().__init__(from_dict=from_dict)


    # --------------------------------------------------------------------------
    @property
    def uid(self):
        return self.payload.uid

    @uid.setter
    def uid(self, value):
        self.payload.uid = value


ru.Message.register_msg_type('heartbeat', HeartbeatMessage)

# ------------------------------------------------------------------------------

