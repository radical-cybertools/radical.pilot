

from typing import Optional, Dict, Tuple, Any

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
#
class RPCRequestMessage(ru.Message):

    # ------------------------------
    class Payload(ru.TypedDict):
        _schema   = {
                        'uid' : str,   # uid of message
                        'addr': str,   # who is expected to act on the request
                        'cmd' : str,   # rpc command
                        'args': dict,  # rpc command arguments
                    }
        _defaults = {
                        'uid' : None,
                        'addr': None,
                        'cmd' : None,
                        'args': {},
                    }
    # ------------------------------

    _schema = {
        'payload': Payload
    }

    _defaults = {
        'msg_type': 'rpc_req',
        'payload' : {}
    }



    # --------------------------------------------------------------------------
    def __init__(self, uid : Optional[str]            = None,
                       addr: Optional[str]            = None,
                       rpc : Optional[str]            = None,
                       args: Optional[Dict[str, Any]] = None):
        '''
        support msg construction and usage like this:

            msg = rp.Message(addr='pilot.0000', rpc='stop')
            assert msg.addr == 'pilot.0000'

        '''

        from_dict = dict()

        if addr: from_dict['addr'] = addr
        if rpc:  from_dict['rpc']  = rpc
        if args: from_dict['args'] = args

        super().__init__(from_dict=from_dict)


    # --------------------------------------------------------------------------
    @property
    def addr(self):
        return self.payload.addr

    @addr.setter
    def addr(self, value):
        self.payload.addr = value


    @property
    def rpc(self):
        return self.payload.rpc

    @rpc.setter
    def rpc(self, value):
        self.payload.rpc = value


    @property
    def args(self):
        return self.payload.args

    @args.setter
    def args(self, value):
        self.payload.args = value


ru.Message.register_msg_type('rpc_req', RPCRequestMessage)


# ------------------------------------------------------------------------------
#
class RPCResultMessage(ru.Message):

    # ------------------------------
    class Payload(ru.TypedDict):
        _schema   = {
                        'uid': str,  # uid of rpc call
                        'val': Any,  # return value (`None` by default)
                        'out': str,  # stdout
                        'err': str,  # stderr
                        'exc': str,  # raised exception representation
                    }
        _defaults = {
                        'uid': None,
                        'val': None,
                        'out': None,
                        'err': None,
                        'exc': None,
                    }
    # ------------------------------

    _schema = {
        'payload': Payload
    }

    _defaults = {
        'msg_type': 'rpc_res',
        'payload' : {}
    }



    # --------------------------------------------------------------------------
    def __init__(self, rpc_req: Optional[RPCRequestMessage] = None,
                       uid    : Optional[str]               = None,
                       val    : Optional[Any]               = None,
                       out    : Optional[str]               = None,
                       err    : Optional[str]               = None,
                       exc    : Optional[Tuple[str, str]]   = None):
        '''
        support rpc response message construction from an rpc request message
        (carries over `uid`):

            msg = rp.Message(rpc_req=req_msg, val=42)

        '''

        from_dict = dict()

        if rpc_req: from_dict['uid'] = rpc_req.uid

        if uid: from_dict['uid'] = uid
        if val: from_dict['val'] = uid
        if out: from_dict['out'] = uid
        if err: from_dict['err'] = uid
        if exc: from_dict['exc'] = uid

        super().__init__(from_dict=from_dict)


    # --------------------------------------------------------------------------
    @property
    def addr(self):
        return self.payload.addr

    @addr.setter
    def addr(self, value):
        self.payload.addr = value


    @property
    def rpc(self):
        return self.payload.rpc

    @rpc.setter
    def rpc(self, value):
        self.payload.rpc = value


    @property
    def args(self):
        return self.payload.args

    @args.setter
    def args(self, value):
        self.payload.args = value


ru.Message.register_msg_type('rpc_req', RPCRequestMessage)

# ------------------------------------------------------------------------------

