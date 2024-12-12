

from typing import Any

import radical.utils as ru


# ------------------------------------------------------------------------------
#
class RPBaseMessage(ru.Message):

    # rpc distinguishes messages which are forwarded to the proxy bridge and
    # those which are not and thus remain local to the module they originate in.

    _msg_type = 'rp_msg'
    _schema   = {'fwd': bool}
    _defaults = {'fwd': False}

    @classmethod
    def register(cls):
        # TODO: this should be moved to the RU base class
        cls._defaults['_msg_type'] = cls._msg_type
        ru.Message.register_msg_type(cls._msg_type, cls)


    # we do not register this message type - it is not supposed to be used
    # directly.


# ------------------------------------------------------------------------------
#
class ComponentStartedMessage(RPBaseMessage):

    # startup messages are never forwarded

    _msg_type = 'component_start'
    _schema   = {'uid': str,
                 'pid': int}
    _defaults = {'fwd': False,
                 'uid': None,
                 'pid': None}


ComponentStartedMessage.register()


# ------------------------------------------------------------------------------
#
class RPCRequestMessage(RPBaseMessage):

    _msg_type = 'rpc_req'
    _schema   = {'uid'      : str,   # uid of message
                 'addr'     : str,   # who is expected to act on the request
                 'cmd'      : str,   # rpc command
                 'args'     : list,  # rpc command arguments
                 'kwargs'   : dict}  # rpc command named arguments
    _defaults = {'fwd'      : True,
                 'uid'      : None,
                 'addr'     : None,
                 'cmd'      : None,
                 'args'     : [],
                 'kwargs'   : {}}


RPCRequestMessage.register()


# ------------------------------------------------------------------------------
#
class RPCResultMessage(RPBaseMessage):

    _msg_type = 'rpc_res'
    _schema   = {'uid'      : str,  # uid of rpc call
                 'val'      : Any,  # return value (`None` by default)
                 'out'      : str,  # stdout
                 'err'      : str,  # stderr
                 'exc'      : str}  # raised exception representation
    _defaults = {'_msg_type': _msg_type,
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


RPCResultMessage.register()


# ------------------------------------------------------------------------------

