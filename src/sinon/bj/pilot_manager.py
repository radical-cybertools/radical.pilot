

import os

import radical.utils  as ru

import pilot          as p
import session        as s
import attributes     as att
import sinon._api     as sa


# ------------------------------------------------------------------------------
#
class PilotManager (att.Attributes, sa.PilotManager) :
    # BigJob has a notion of pilot service, but it has different semantics than
    # in out API -- we need to keep one bigjob pilot service around for every
    # pilot we create.  We thus don't have any BJ entity representing our
    # PilotManager class, really.  So, this is a mostly empty class which
    # forwards calls to the pilot.
    #
    # The class will, however, interpret the submitted pilot descriptions to
    # some extent, and in particular will transform the RESOURCE key according
    # to an configuration file (adding resource access schema etc as needed).

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pmid=None, session=None) : 

        # make sure BJ can operate
        if  not 'COORDINATION_URL' in os.environ :
            raise RuntimeError ("COORDINATION_URL not set")
        self.coord = os.environ['COORDINATION_URL']

        # initialize session
        self._sid = s.initialize ()

        # get a unique ID if none was given -- otherwise we reconnect
        if  not pmid :
            self.pmid = ru.generate_id ('pm.')
        else :
            self.pmid = str(pmid)

        # initialize attributes
        att.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # deep inspection
        self._attributes_register  ('pmid', self.pmid, att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.PILOTS,     [], att.STRING, att.VECTOR, att.READONLY)
        # ...

        # when starting pilots, we need to map the given RESOURCE keys to bigjob
        # compatible URLs.  That mapping is done via a json config file -- which
        # we read here
        cfg_location = os.path.dirname (__file__) + '/resource.cfg'
        self._resource_cfg = ru.read_json (cfg_location)

        # FIXME: ensure that description is suitable for BJ.  Translate as
        # necessary (slots).  Posisbly use the above config for deriving wayness
        # etc.


    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description) :

        if  not sa.RESOURCE in description :
            raise ValueError ("no RESOURCE specified in pilot description")

        # replace resource with more complete spec, if so configured 
        if  description[sa.RESOURCE] in self._resource_cfg :
            description[sa.RESOURCE] = self._resource_cfg[description[sa.RESOURCE]]

        print description


        # hand off pilot creation to the pilot class -- which will create
        # a dedicated BJ pilot service for doing so
        pilot = p.Pilot._create (description, self)

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
    def wait_pilot (self, pids, state=[sa.DONE, sa.FAILED, sa.CANCELED], 
                    timeout=-1.0) :

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
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

