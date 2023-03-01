#!/usr/bin/env python3

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    session = None
    try:
        session = rp.Session()
        sid     = session.uid
        pmgr    = rp.PilotManager(session)
        pdesc   = rp.PilotDescription({'resource': 'local.localhost'})
        pilot   = pmgr.submit_pilots([pdesc])[0]
        result  = pilot.rpc(rpc='hello', args=['world'])
        assert result == 'hello world'

    finally:
        if session:
            session.close(download=False)
            cmd = 'rm -r ./rp.session.%s/' % session.uid[11:]
            ru.sh_callout(cmd)


# ------------------------------------------------------------------------------

