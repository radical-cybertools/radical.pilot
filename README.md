saga-pilot
==========

This is a module based SAGA-Pilot implementation, used for RADICAL research
purposes.  This version is *not* for for external / production usage!


# Sinon -- the SAGA Pilot Prototype Implementation

To run the `test.py` example, you should set the following environment
variables::

    export SINON_SESSION_ID=s.1
    export SINON_USER=merzky
    export REDIS_URL=redis://repex1.tacc.utexas.edu:10001/

(with a sensible set of values obviously).  Note that using an existing
username/session/redis_url pair will re-use (and possibly overwrite) existing
entries -- so please be aware of that...

