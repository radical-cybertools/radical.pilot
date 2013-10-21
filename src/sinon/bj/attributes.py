

import saga.attributes  as satt
import sinon._api       as sa


# ------------------------------------------------------------------------------
#
ANY         = satt.ANY
URL         = satt.URL
INT         = satt.INT
FLOAT       = satt.FLOAT
STRING      = satt.STRING
BOOL        = satt.BOOL
ENUM        = satt.ENUM
TIME        = satt.TIME

WRITEABLE   = satt.WRITEABLE
READONLY    = satt.READONLY
FINAL       = satt.FINAL
ALIAS       = satt.ALIAS

EXTENDED    = satt.EXTENDED
PRIVATE     = satt.PRIVATE

SCALAR      = satt.SCALAR
DICT        = satt.DICT
VECTOR      = satt.VECTOR


# ------------------------------------------------------------------------------
#
class Attributes (satt.Attributes, sa.Attributes) :

    def __init__ (self, *args, **kwargs) :

        satt.Attributes.__init__ (self, *args, **kwargs)

    def __str__ (self) :
        return str (self.as_dict ())


print 'have Attributes'
# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

