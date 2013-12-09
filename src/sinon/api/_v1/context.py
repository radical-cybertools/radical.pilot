

import saga.context    as sc
import sinon._api      as sa


# ------------------------------------------------------------------------------
#
class Context (sc.Context, sa.Context) : 

    def __init__ (self, ctype) :

        sc.Context.__init__ (self, ctype)


# ------------------------------------------------------------------------------
#


