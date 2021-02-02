#!/usr/bin/env python3

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    sid = None
    try:
        session = rp.Session()
        sid     = session.uid
        pmgr    = rp.PilotManager(session)
        pdesc   = rp.PilotDescription({'resource': 'local.localhost'})
        pilot   = pmgr.submit_pilots(pdesc)
        result  = pilot.rpc(rpc='hello', args=['world'])
        assert(result == 'hello world')

    finally:
        if sid and sid.startswith('rp.session'):
            session.close(download=False)
            cmd = 'rm -r ./rp.session.%s/' % sid[11:]
            ru.sh_callout(cmd)


# ------------------------------------------------------------------------------

