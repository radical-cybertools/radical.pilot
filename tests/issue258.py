import os
import radical.pilot as rp

DBURL = os.getenv("RADICAL_PILOT_DBURL")

if __name__ == "__main__":

    session = rp.Session(database_url=DBURL)
    pmgr    = rp.PilotManager(session=session)
    pmgr.wait_pilots(pilot_ids="12", state=rp.ACTIVE)

