

import sinon._api      as sa
import saga.url        as su


# ------------------------------------------------------------------------------
#
class Url (su.Url, sa.Url) : 

    def __init__ (self, url_string='') :

        su.Url.__init__ (self, url_string)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

