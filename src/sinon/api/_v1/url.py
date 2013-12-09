

import saga.url    as su
import sinon._api      as sa


# ------------------------------------------------------------------------------
#
class Url (su.Url, sa.Url) : 

    def __init__ (self, url_string='') :

        su.Url.__init__ (self, url_string)


# ------------------------------------------------------------------------------
#


