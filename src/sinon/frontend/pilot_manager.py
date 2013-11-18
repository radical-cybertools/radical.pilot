"""
.. module:: sinon.pilot_manager
   :platform: Unix
   :synopsis: Implementation of the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, radical.rutgers.edu"
__license__   = "MIT"

from sinon.constants  import *
from sinon.utils      import as_list
from sinon.exceptions import SinonException

from sinon.frontend.session      import Session
from sinon.frontend.pilot        import Pilot

from sinon.db import Session as dbSession

from radical.utils import which

import saga
import json
import urllib2
import datetime


# ------------------------------------------------------------------------------
#
class PilotManager(object):
    """A PilotManager holds :class:`sinon.Pilot` instances that are 
    submitted via the :meth:`sinon.PilotManager.submit_pilots` method.
    
    It is possible to attach one or more :ref:`chapter_machconf` 
    to a PilotManager to outsource machine specific configuration 
    parameters to an external configuration file. 

    Each PilotManager has a unique identifier :data:`sinon.PilotManager.uid`
    that can be used to re-connect to previoulsy created PilotManager in a
    given :class:`sinon.Session`.

    **Example**::

        s = sinon.Session(database_url=DBURL)
        
        pm1 = sinon.PilotManager(session=s, resource_configurations=RESCONF)
        # Re-connect via the 'get()' method.
        pm2 = sinon.PilotManager.get(session=s, pilot_manager_uid=pm1.uid)

        # pm1 and pm2 are pointing to the same PilotManager
        assert pm1.uid == pm2.uid
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, session, resource_configurations): 
        """Creates a new PilotManager and attaches is to the session. 

        .. note:: The `resource_configurations` (see :ref:`chapter_machconf`)
                  parameter is currently mandatory for creating a new 
                  PilotManager instance. 

        **Arguments:**

            * **session** [:class:`sinon.Session`]: 
              The session instance to use.

            * **resource_configurations** [`string` or `list of strings`]: 
              A list of URLs pointing to :ref:`chapter_machconf`. Currently 
              `file://`, `http://` and `https://` URLs are supported.
              
              If one or more resource_configurations are provided, Pilots
              submitted  via this PilotManager can access the configuration
              entries in the  files via the :class:`ComputePilotDescription`.
              For example::

                  pm = sinon.PilotManager(session=s, resource_configurations="https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json")

                  pd = sinon.ComputePilotDescription()
                  pd.resource = "futuregrid.INDIA"  # defined in futuregrid.json
                  pd.cores = 16

                  pilot_india = pm.submit_pilots(pd)

        **Returns:**

            * A new `PilotManager` object [:class:`sinon.PilotManager`].

        **Raises:**
            * :class:`sinon.SinonException`
        """
        self._DB = session._dbs
        self._session = session

        if resource_configurations == "~=RECON=~":
            # When we get the "~=RECON=~" keyword as resource_configurations, we
            # were called  from the 'get()' class method. In this case object 
            # instantiation happens there... 
            return

        ###############################
        # Create a new pilot manager. #
        ###############################

        # Donwload and parse the configuration file(s) and the content to 
        # our resource dictionary.
        self._resource_cfgs = {}
        for rcf in as_list(resource_configurations):
            try:
                # download resource configuration file
                response = urllib2.urlopen(rcf)
                rcf_content = response.read()
            except urllib2.URLError, err:
                raise SinonException("Couln't open/download resource configuration file '%s': %s." % (rcf, str(err)))

            try:
                # convert JSON string to dictionary and append
                rcf_dict = json.loads(rcf_content)
                for key, val in rcf_dict.iteritems():
                    if key in self._resource_cfgs:
                        raise SinonException("Resource configuration entry for '%s' defined in %s is already defined." % (key, rcf))
                    self._resource_cfgs[key] = val
            except ValueError, err:
                raise SinonException("Couldn't parse resource configuration file '%s': %s." % (rcf, str(err)))

        self._uid = self._DB.insert_pilot_manager(pilot_manager_data={})

    # --------------------------------------------------------------------------
    #
    @classmethod 
    def get(cls, session, pilot_manager_uid) :
        """ Re-connects to an existing PilotManager via its uid.

        **Arguments:**

            * **session** [:class:`sinon.Session`]: 
              The session instance to use.

            * **pilot_manager_uid** [`string`]: 
              The unique identifier of the PilotManager we want 
              to re-connect to.

        **Returns:**

            * A new `PilotManager` object [:class:`sinon.PilotManager`].

        **Raises:**

            * :class:`sinon.SinonException` if a PilotManager with 
              `pilot_manager_uid` doesn't exist in the database.
        """

        ###########################################
        # Reconnect to an existing pilot manager. #
        ###########################################

        if pilot_manager_uid not in session._dbs.list_pilot_manager_uids():
            raise LookupError ("PilotManager '%s' not in database." \
                % pilot_manager_uid)

        obj = cls(session=session, resource_configurations="~=RECON=~")
        obj._uid = pilot_manager_uid
        obj._resource_cfgs = None # TODO: reconnect

        return obj

    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the PilotManagers's unique identifier.

        The uid identifies the PilotManager within the :class:`sinon.Session`
        and can be used to retrieve an existing PilotManager.

        **Returns:**

            * A unique identifier [`string`].
        """
        return self._uid

    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """Submits a new :class:`sinon.Pilot` to a resource. 

        **Returns:**

            * One or more :class:`sinon.Pilot` instances 
              [`list of :class:`sinon.Pilot`].

        **Raises:**

            * :class:`sinon.SinonException`
        """

        # Sumbit pilots is always a bulk operation. In bulk submission, it
        # doesn't make too much sense to throw  exceptions if a pilot fails to
        # launch, e.g., if you submit 10 pilots, 9 start properly and only one
        # fails, you don't want submit_pilots to  throw an exception. instead of
        # throwing exceptions, we rather  set the pilot's state to 'FAILED' and
        # add the submission errror  which we would have thrown as an exception
        # otherwise, to the pilot's  log.

        from bson.objectid import ObjectId

        # implicit -> list -> dict conversion
        pilot_description_dict = {}
        for pd in as_list(pilot_descriptions):
            pilot_description_dict[ObjectId()] = {
                'description': pd, 
                'info': {'log': []}
            }

        # create the pilot objects, launch the actual pilots via saga
        # and update the status accordingly.
        pilots = []
        for pilot_id, pilot_description in pilot_description_dict.iteritems():
            # check wether pilot description defines the mandatory fields 
            resource_key = pilot_description['description'].resource
            number_cores = pilot_description['description'].cores

            #########################
            # Check job description # 
            #########################

            if resource_key is None:
                raise SinonException("ComputePilotDescription.resource not defined")
            # check wether mandatory attribute 'resource' was defined
            if resource_key not in self._resource_cfgs:
                raise SinonException("ComputePilotDescription.resource key '%s' is not known by this PilotManager." % resource_key)
            # check wether mandatory attribute 'cores' was defined
            if number_cores is None:
                raise SinonException("ComputePilotDescription.cores not defined")

            resource_cfg = self._resource_cfgs[resource_key]

            pilot = Pilot._create(
                pilot_uid=str(pilot_id),
                pilot_description=pilot_description['description'], 
                pilot_manager_obj=self)

            ########################################################
            # Create SAGA Job description and submit the pilot job #
            ########################################################
            try:
                # Create working directory if it doesn't exist and copy
                # the agent bootstrap script into it. 
                #
                # We create a new sub-driectory for each agent. each 
                # agent will bootstrap its own virtual environment in this 
                # directory.
                #
                agent_dir_url = saga.Url("%s/pilot-%s/" \
                    % (resource_cfg['working_directory'], str(pilot_id)))

                agent_dir = saga.filesystem.Directory(agent_dir_url, 
                    saga.filesystem.CREATE_PARENTS)
                pilot_description_dict[pilot_id]['info']['log'].append("Created agent directory '%s'" % str(agent_dir_url))

                bootstrap_script_url = saga.Url("file://localhost/%s" \
                    % (which('bootstrap-and-run-agent')))

                bootstrap_script = saga.filesystem.File(bootstrap_script_url)
                bootstrap_script.copy(agent_dir_url)
                pilot_description_dict[pilot_id]['info']['log'].append("Copied launch script '%s' to agent directory" % str(bootstrap_script_url))

                # extract the required connection parameters and uids
                # for the agent:
                database_host = self._session._database_url.split("://")[1]
                database_name = self._session._database_name
                session_uid  = self._session.uid

                # now that the script is in place and we know where it is,
                # we can launch the agent
                js = saga.job.Service(resource_cfg['URL'])

                jd = saga.job.Description()
                jd.working_directory = agent_dir_url.path
                jd.executable        = "./bootstrap-and-run-agent"
                jd.arguments         = ["-r", database_host,  # database host (+ port)
                                        "-n", database_name,  # database name
                                        "-s", session_uid,    # session uid
                                        "-p", str(pilot_id)]  # pilot uid
                jd.output            = "STDOUT"
                jd.error             = "STDERR"
                jd.total_cpu_count   = number_cores

                pilotjob = js.create_job(jd)
                pilotjob.run()

                pilotjob_id = pilotjob.id

                # clean up / close all saga objects
                js.close()
                agent_dir.close()
                bootstrap_script.close()

                # at this point, submission has succeeded. we can update
                #   * the state to 'PENDING'
                #   * the submission time
                #   * the log
                pilot_description_dict[pilot_id]['info']['state'] = PENDING
                pilot_description_dict[pilot_id]['info']['submitted'] = datetime.datetime.now()
                pilot_description_dict[pilot_id]['info']['log'].append("Pilot Job successfully submitted with JobID '%s'" % pilotjob_id)

            except saga.SagaException, se: 
                # at this point, submission has failed. we update the 
                # agent status accordingly
                pilot_description_dict[pilot_id]['info']['state'] = FAILED
                pilot_description_dict[pilot_id]['info']['submitted'] = datetime.datetime.now()
                pilot_description_dict[pilot_id]['info']['log'].append("Pilot Job submission failed: '%s'" % str(se))

            pilots.append(pilot)

        # bulk-create database entries
        self._session._dbs.insert_pilots(pilot_manager_uid=self.uid, 
            pilot_descriptions=pilot_description_dict)

        # implicit return value conversion
        if len(pilots) == 1:
            return pilots[0]
        else:
            return pilots

    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the unique identifiers of all :class:`sinon.Pilot` instances 
        associated with this PilotManager

        **Returns:**

            * A list of :class:`sinon.Pilot` uids [`string`].

        **Raises:**

            * :class:`sinon.SinonException`
        """
        return self._session._dbs.list_pilot_uids(self._uid)

    # --------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_uids=None):
        """Returns one or more :class:`sinon.Pilot` instances.

        **Arguments:**

            * **pilot_uids** [`list of strings`]: If pilot_uids is set, 
              only the Pilots with  the specified uids are returned. If 
              pilot_uids is `None`, all Pilots are returned.

        **Returns:**

            * A list of :class:`sinon.Pilot` objects 
              [`list of :class:`sinon.Pilot`].

        **Raises:**

            * :class:`sinon.SinonException`
        """
        # implicit list conversion
        pilot_uid_list = as_list(pilot_uids)

        pilots = Pilot._get(pilot_uids=pilot_uid_list, pilot_manager_obj=self)
        return pilots

    # --------------------------------------------------------------------------
    #
    def wait_pilots(self, pilot_uids=None, state=[DONE, FAILED, CANCELED], timeout=-1.0):
        """Returns when one or more :class:`sinon.Pilots` reach a 
        specific state. 

        If `pilot_uids` is `None`, `wait_pilots` returns when **all** Pilots
        reach the state defined in `state`. 

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`] 
              If pilot_uids is set, only the Pilots with the specified uids are
              considered. If pilot_uids is `None` (default), all Pilots are 
              considered.

            * **state** [`string`]
              The state that Pilots have to reach in order for the call
              to return. 

              By default `wait_pilots` waits for the Pilots to reach 
              a terminal state, which can be one of the following:

              * :data:`sinon.DONE`
              * :data:`sinon.FAILED`
              * :data:`sinon.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **-1.0** waits forever.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        pass

    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, pilot_uids=None):
        """Cancels one or more Pilots. 

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`] 
              If pilot_uids is set, only the Pilots with the specified uids are
              canceled. If pilot_uids is `None`, all Pilots are canceled.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        pass


