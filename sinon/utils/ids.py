

import random
import datetime

# ------------------------------------------------------------------------------
#
def _generate_xid () :

    now = datetime.datetime.utcnow ()

    xid_date = "%04d%02d%02d" % (now.year, now.month,  now.day)
    xid_time = "%02d%02d%02d" % (now.hour, now.minute, now.second)
    xid_rand = "%04d"         % (random.randint (0, 9999))

    xid = "%s.%s.%s" % (xid_date, xid_time, xid_rand)
    xid = "%s.%s"    % (xid_date,           xid_rand)

    return xid


# ------------------------------------------------------------------------------
#
def generate_session_id () :

    return 'sid.' + _generate_xid ()


# ------------------------------------------------------------------------------
#
def generate_unit_id () :

    return 'uid.' + _generate_xid ()


# ------------------------------------------------------------------------------
#
def generate_pilot_id () :

    return 'pid.' + _generate_xid ()

