

import sinon.api       as sa
import saga.task       as st


# ------------------------------------------------------------------------------
#
class Task (st.Task, sa.Task) :

    def __init__ (self) :

        st.Task.__init__ (self)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

