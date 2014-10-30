
import sys
import radical.pilot as rp

if __name__ == "__main__":

    session = rp.Session()
    pmgr = rp.PilotManager(session=session)
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "local.localhost"
    pdesc.runtime  = 10
    pdesc.cores    = 1
    pilot = pmgr.submit_pilots(pdesc)
    pmgr.cancel_pilots()
    pmgr.wait_pilots (state=rp.CANCELED)

    print "\n== PILOT STATE HISTORY==\n"

    print "pilot %s" % pilot.uid
    states = list()
    for entry in pilot.state_history:
        print " * %s: %s" % (entry.timestamp, entry.state)
        states.append (entry.state)
    assert (rp.PENDING_LAUNCH in states)
    assert (rp.LAUNCHING      in states)
    assert (rp.PENDING_ACTIVE in states)
    assert (rp.ACTIVE         in states)
    assert (rp.CANCELED       in states)

    # Remove session from database
    session.close()

