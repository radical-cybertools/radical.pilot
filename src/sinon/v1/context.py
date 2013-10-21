

import sinon._api      as sa
import saga.context    as sc


# ------------------------------------------------------------------------------
#
class Context (sc.Context, sa.Context) : 

    def __init__ (self, ctype) :

        sc.Context.__init__ (self, ctype)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

