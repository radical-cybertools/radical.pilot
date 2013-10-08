

import random
import datetime
import threading


# ------------------------------------------------------------------------------
#
_rlock = threading.RLock ()

with _rlock :
    _id_cnts = {}
    

# ------------------------------------------------------------------------------
#
def _generate_xid (idtype) :

    with _rlock :

        global _id_cnts

        now = datetime.datetime.utcnow ()

        if  not idtype in _id_cnts :
            _id_cnts[idtype] =  1
        else :
            _id_cnts[idtype] += 1

        xid_date = "%04d%02d%02d" % (now.year, now.month,  now.day)
        xid_time = "%02d%02d%02d" % (now.hour, now.minute, now.second)
        xid_seq  = "%04d"         % (_id_cnts[idtype])

        # session IDs need to be somewhat unique
        if  idtype == 's' : 
            xid = "%s.%s.%s.%s" % (idtype, xid_date, xid_time, xid_seq)
        else :
            xid = "%s.%s"       % (idtype,                     xid_seq)

        print 'xid: %s' % xid

        return str(xid)


# ------------------------------------------------------------------------------
#
def generate_session_id () :

    return _generate_xid ('s')


# ------------------------------------------------------------------------------
#
def generate_unit_manager_id () :

    return _generate_xid ('um')


# ------------------------------------------------------------------------------
#
def generate_pilot_manager_id () :

    return _generate_xid ('pm')


# ------------------------------------------------------------------------------
#
def generate_unit_id () :

    return _generate_xid ('u')


# ------------------------------------------------------------------------------
#
def generate_pilot_id () :

    return _generate_xid ('p')


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

