

import sinon._api      as sa
import saga.session    as ss


# ------------------------------------------------------------------------------
#
class Session (ss.Session, sa.Session) :

    def __init__ (self, default=True) :

        ss.Session.__init__ (self, default)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

