

from typing import Any

import radical.utils as ru

# ------------------------------------------------------------------------------
#
class RPBaseMessage(ru.Message):

    # rpc distinguishes messages which are forwarded to the proxy bridge and
    # those which are not and thus remain local to the module they originate in.

    _schema   = {'fwd'      : bool}
    _defaults = {'_msg_type': 'rp_msg',
                 'fwd'      : False}


    # we do not register this message type - it is not supposed to be used
    # directly.


# ------------------------------------------------------------------------------
#
class HeartbeatMessage(RPBaseMessage):

    # heartbeat messages are never forwarded

    _schema   = {'uid'      : str}
    _defaults = {'_msg_type': 'heartbeat',
                 'fwd'      : False,
                 'uid'      : None}


ru.Message.register_msg_type('heartbeat', HeartbeatMessage)


# ------------------------------------------------------------------------------
#
class RPCRequestMessage(RPBaseMessage):

    _schema   = {'uid'      : str,   # uid of message
                 'addr'     : str,   # who is expected to act on the request
                 'cmd'      : str,   # rpc command
                 'args'     : list,  # rpc command arguments
                 'kwargs'   : dict}  # rpc command named arguments
    _defaults = {
                 '_msg_type': 'rpc_req',
                 'fwd'      : True,
                 'uid'      : None,
                 'addr'     : None,
                 'cmd'      : None,
                 'args'     : [],
                 'kwargs'   : {}}



ru.Message.register_msg_type('rpc_req', RPCRequestMessage)


# ------------------------------------------------------------------------------
#
class RPCResultMessage(RPBaseMessage):

    _schema   = {'uid'      : str,  # uid of rpc call
                 'val'      : Any,  # return value (`None` by default)
                 'out'      : str,  # stdout
                 'err'      : str,  # stderr
                 'exc'      : str}  # raised exception representation
    _defaults = {'_msg_type': 'rpc_res',
                 'fwd'      : True,
                 'uid'      : None,
                 'val'      : None,
                 'out'      : None,
                 'err'      : None,
                 'exc'      : None}

    # --------------------------------------------------------------------------
    #
    def __init__(self, rpc_req=None, from_dict=None, **kwargs):

        # when constructed from a request message copy the uid

        if rpc_req:
            if not from_dict:
                from_dict = dict()

            from_dict['uid'] = rpc_req['uid']

        super().__init__(from_dict, **kwargs)


ru.Message.register_msg_type('rpc_res', RPCResultMessage)


# ------------------------------------------------------------------------------

