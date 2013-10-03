

import sinon.api       as sa
import saga.attributes as satt


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


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

