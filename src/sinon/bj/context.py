

import saga.context    as sc
import sinon.api       as sa


# ------------------------------------------------------------------------------
#
class Context (sc.Context, sa.Context) : 

    def __init__ (self, ctype) :

        sc.Context.__init__ (self, ctype)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

