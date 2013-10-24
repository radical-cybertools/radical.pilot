

import os

import sinon.v1.pilot           as p
import sinon.v1.session         as s
import sinon.v1.attributes      as att
import sinon._api      as sa

import uuid
import sinon._api as sa
from sinon.db import Session as dbSession



# ------------------------------------------------------------------------------
#
class PilotManager (att.Attributes, sa.PilotManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pmid=None, session=None) : 

        if pmid is None:
            # if session_id is 'None' we create a new session
            pmid = str(uuid.uuid4())
            #self._dbs = dbSession.new(sid=session_id, db_url=database_url, db_name=database_name)
        #else:
            # otherwise, we reconnect to an exissting session
            #self._dbs = dbSession.reconnect(sid=session_id, db_url=database_url, db_name=database_name)


        # get a unique ID if none was given -- otherwise we reconnect
        #if  not pmid :
        #    self.pmid = ru.generate_id ('pm.')
        #else :
        #    self.pmid = str(pmid)

        # initialize attributes
        att.Attributes.__init__ (self)

        # set attribute interface properties
        #self._attributes_extensible  (False)
        #self._attributes_camelcasing (True)

        # deep inspection
        #self._attributes_register  ('pmid',    self.pmid, att.STRING, att.SCALAR, att.READONLY)
        #self._attributes_register  (sa.PILOTS, [],        att.STRING, att.VECTOR, att.READONLY)
        # ...

        # when starting pilots, we need to map the given RESOURCE keys to
        # endpoint compatible URLs.  That mapping is done via a json config
        # file -- which we read here
        #cfg_location = os.path.dirname (__file__) + '/resource.cfg'
        #self._resource_cfg = ru.read_json (cfg_location)


    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description) :

        # FIXME: bulk

        if  not sa.RESOURCE in description :
            raise ValueError ("no RESOURCE specified in pilot description")

        # replace resource with more complete spec, if so configured 
        if  description[sa.RESOURCE] in self._resource_cfg :
            description[sa.RESOURCE] = self._resource_cfg[description[sa.RESOURCE]]

        print description


        # hand off pilot creation to the pilot class
        pilot  = p.Pilot._create (description, self)

        # keep pilot around for inspection
        self.pilots.append (pilot.pid)

        return pilot


    # --------------------------------------------------------------------------
    #
    def list_pilots (self) :

        return self.pilots


    # --------------------------------------------------------------------------
    #
    def get_pilot (self, pids) :

        if  not isinstance (pids, list) :
            if  not pids in self.pilots :
                raise LookupError ("pilot '%s' not found" % pids)

            # FIXME: switch by type
            return troy.ComputePilot (pids)

        # handle bulk
        else :
            ret = []

            for pid in pids :
                if  not pid in self.pilots :
                    raise LookupError ("pilot '%s' not found" % pid)

                # FIXME: switch by type
                ret.append (troy.ComputePilot (pid))

            return ret



    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=[sa.DONE, sa.FAILED, sa.CANCELED], timeout=-1.0) :

        if  not isinstance (state, list) :
            state = [state]


        if  not isinstance (pids, list) :
            if  not pids in self.pilots :
                raise LookupError ("pilot '%s' not found" % pids)

            troy.Pilot (pids).wait (state, timeout)

        # handle bulk
        else :
            # FIXME: better timeout handling
            for pid in pids :
                if  not pid in self.pilots :
                    raise LookupError ("pilot '%s' not found" % pid)

                troy.Pilot (pid).wait (state, timeout)


    # --------------------------------------------------------------------------
    #
    def cancel_pilot (self, pids) :

        if  not isinstance (pids, list) :
            if  not pids in self.pilots :
                raise LookupError ("pilot '%s' not found" % pids)

            troy.Pilot (pids).cancel ()

        # handle bulk
        else :
            for pid in pids :
                if  not pid in self.pilots :
                    raise LookupError ("pilot '%s' not found" % pid)

                troy.Pilot (pid).cancel ()


# ------------------------------------------------------------------------------
#


