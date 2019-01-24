#!/usr/bin/env python


import radical.pilot as rp

session = rp.Session()

try:

    pmgr = rp.PilotManager(session=session)
    pmgr.wait_pilots(pilot_ids="12", state=rp.ACTIVE)
    print "TEST FAILED"

except KeyError as e:
    print "TEST SUCCESS"

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()

