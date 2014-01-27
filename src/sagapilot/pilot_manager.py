"""
.. module:: sagapilot.pilot_manager
   :platform: Unix
   :synopsis: Implementation of the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from sagapilot.compute_pilot import ComputePilot
from sagapilot.utils.logger      import logger

import sagapilot.states     as states
import sagapilot.attributes as attributes
import sagapilot.exceptions as exceptions

from bson.objectid import ObjectId
from radical.utils import which

import os
import saga
import time
import json
import urllib2
import datetime

# ------------------------------------------------------------------------------
# Attribute keys
UID  = 'UID'

# ------------------------------------------------------------------------------
#
class PilotManager(attributes.Attributes):
    """A PilotManager holds :class:`sagapilot.ComputePilot` instances that are 
    submitted via the :meth:`sagapilot.ComputePilotManager.submit_pilots` method.
    
    It is possible to attach one or more :ref:`chapter_machconf` 
    to a PilotManager to outsource machine specific configuration 
    parameters to an external configuration file. 

    Each PilotManager has a unique identifier :data:`sagapilot.ComputePilotManager.uid`
    that can be used to re-connect to previoulsy created PilotManager in a
    given :class:`sagapilot.Session`.

    **Example**::

        s = sagapilot.Session(database_url=DBURL)
        
        pm1 = sagapilot.ComputePilotManager(session=s, resource_configurations=RESCONF)
        # Re-connect via the 'get()' method.
        pm2 = sagapilot.ComputePilotManager.get(session=s, pilot_manager_id=pm1.uid)

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

            * **session** [:class:`sagapilot.Session`]: 
              The session instance to use.

            * **resource_configurations** [`string` or `list of strings`]: 
              A list of URLs pointing to :ref:`chapter_machconf`. Currently 
              `file://`, `http://` and `https://` URLs are supported.
              
              If one or more resource_configurations are provided, Pilots
              submitted  via this PilotManager can access the configuration
              entries in the  files via the :class:`ComputePilotDescription`.
              For example::

                  pm = sagapilot.ComputePilotManager(session=s, resource_configurations="https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json")

                  pd = sagapilot.ComputePilotDescription()
                  pd.resource = "futuregrid.INDIA"  # defined in futuregrid.json
                  pd.cores = 16

                  pilot_india = pm.submit_pilots(pd)

        **Returns:**

            * A new `PilotManager` object [:class:`sagapilot.ComputePilotManager`].

        **Raises:**
            * :class:`sagapilot.SinonException`
        """
        # Each pilot manager has a worker thread associated with it. The task of the 
        # worker thread is to check and update the state of pilots, fire callbacks
        # and so on. 

        self._DB = session._dbs
        self._session = session

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible(False)
        self._attributes_camelcasing(True)

        # The UID attributes
        self._attributes_register(UID, None, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(UID, self._get_uid_priv)

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
        
        if not isinstance(resource_configurations, list):
            resource_configurations = [resource_configurations]
        
        for rcf in resource_configurations:
            try:
                # download resource configuration file
                response = urllib2.urlopen(rcf)
                rcf_content = response.read()
            except urllib2.URLError, err:
                msg = "Couln't open/download resource configuration file '%s': %s." % (rcf, str(err))
                raise exceptions.BadParameter(msg=msg)

            try:
                # convert JSON string to dictionary and append
                rcf_dict = json.loads(rcf_content)
                for key, val in rcf_dict.iteritems():
                    if key in self._resource_cfgs:
                        raise exceptions.BadParameter("Resource configuration entry for '%s' defined in %s is already defined." % (key, rcf))
                    self._resource_cfgs[key] = val
            except ValueError, err:
                raise exceptions.BadParameter("Couldn't parse resource configuration file '%s': %s." % (rcf, str(err)))

        self._uid = self._DB.insert_pilot_manager(pilot_manager_data={})

        logger.info("Created new PilotManager %s." % str(self))

    #---------------------------------------------------------------------------
    #
    @classmethod
    def _reconnect(cls, session, pilot_manager_id):

        if pilot_manager_id not in session._dbs.list_pilot_manager_uids():
            raise exceptions.BadParameter ("PilotManager with id '%s' not in database." % pilot_manager_id)

        obj = cls(session=session, resource_configurations="~=RECON=~")
        obj._uid           = pilot_manager_id
        obj._resource_cfgs = None # TODO: reconnect

        logger.info("Reconnected to existing PilotManager %s." % str(obj))

        return obj

    #---------------------------------------------------------------------------
    #
    def _get_uid_priv(self):
        """Returns the PilotManagers's unique identifier.

        The uid identifies the PilotManager within the :class:`sagapilot.Session`
        and can be used to retrieve an existing PilotManager.

        **Returns:**

            * A unique identifier [`string`].
        """
        return self._uid

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns information about the pilot manager as a Python dictionary.
        """
        info_dict = {
            'type' : 'PilotManager', 
            'uid'   : self._get_uid_priv()
        }
        return info_dict

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the pilot manager.
        """
        return str(self.as_dict())

    # --------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """Submits a new :class:`sagapilot.ComputePilot` to a resource. 

        **Returns:**

            * One or more :class:`sagapilot.ComputePilot` instances 
              [`list of :class:`sagapilot.ComputePilot`].

        **Raises:**

            * :class:`sagapilot.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")
        
        if not isinstance(pilot_descriptions, list):
            pilot_descriptions = [pilot_descriptions]

        # Sumbit pilots is always a bulk operation. In bulk submission, it
        # doesn't make too much sense to throw  exceptions if a pilot fails to
        # launch, e.g., if you submit 10 pilots, 9 start properly and only one
        # fails, you don't want submit_pilots to  throw an exception. instead of
        # throwing exceptions, we rather  set the pilot's state to 'FAILED' and
        # add the submission errror  which we would have thrown as an exception
        # otherwise, to the pilot's  log.

        pilot_description_dict = {}
        for pd in pilot_descriptions:
            pilot_description_dict[ObjectId()] = {
                'description' : pd, 
                'command'     : None, 
                'info'        : {'state'     : None, 
                                 'submitted' : None,
                                 'started'   : None,
                                 'finished'  : None,
                                 'log'       : []
                                }
            }

        # create the pilot objects, launch the actual pilots via saga
        # and update the status accordingly.
        pilots = []
        for pilot_id, pilot_description in pilot_description_dict.iteritems():
            # check wether pilot description defines the mandatory fields 
            resource_key = pilot_description['description'].resource
            number_cores = pilot_description['description'].cores
            run_time     = pilot_description['description'].run_time
            queue        = pilot_description['description'].queue

            try_submit = True

            #########################
            # Check job description # 
            #########################

            if resource_key is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'resource'."
                pilot_description_dict[pilot_id]['info']['state'] = states.FAILED
                pilot_description_dict[pilot_id]['info']['log'].append(error_msg)
                pilot_description_dict[pilot_id]['info']['submitted'] = datetime.datetime.utcnow()
                try_submit = False
                raise exceptions.BadParameter(error_msg)

            # check wether mandatory attribute 'resource' was defined
            if resource_key not in self._resource_cfgs:
                error_msg = "ComputePilotDescription.resource key '%s' is not known by this PilotManager." % resource_key
                pilot_description_dict[pilot_id]['info']['state'] = states.FAILED
                pilot_description_dict[pilot_id]['info']['log'].append(error_msg)
                pilot_description_dict[pilot_id]['info']['submitted'] = datetime.datetime.utcnow()
                try_submit = False
                raise exceptions.BadParameter(error_msg)

            # check wether mandatory attribute 'cores' was defined
            if number_cores is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'cores'."
                pilot_description_dict[pilot_id]['info']['state'] = states.FAILED
                pilot_description_dict[pilot_id]['info']['log'].append(error_msg)
                pilot_description_dict[pilot_id]['info']['submitted'] = datetime.datetime.utcnow()
                try_submit = False
                raise exceptions.BadParameter(error_msg)

            # check wether mandatory attribute 'run_time' was defined
            if run_time is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'run_time'."
                pilot_description_dict[pilot_id]['info']['state'] = states.FAILED
                pilot_description_dict[pilot_id]['info']['log'].append(error_msg)
                pilot_description_dict[pilot_id]['info']['submitted'] = datetime.datetime.utcnow()
                try_submit = False
                raise exceptions.BadParameter(error_msg)

            # check wether the resource key actually exists
            if resource_key not in self._resource_cfgs:
                error_msg = "No entry found for resource key '%s' in resource configuration." % resource_key
                pilot_description_dict[pilot_id]['info']['state'] = states.FAILED
                pilot_description_dict[pilot_id]['info']['log'].append(error_msg)
                pilot_description_dict[pilot_id]['info']['submitted'] = datetime.datetime.utcnow()
                try_submit = False
                raise exceptions.BadParameter(error_msg)
            else:
                resource_cfg = self._resource_cfgs[resource_key]

            # only try to submit pilot of all required attributes are in place
            if try_submit is True:
                ########################################################
                # Create SAGA Job description and submit the pilot job #
                ########################################################
                try:
                    # create a custom SAGA Session and add the credentials
                    # that are attached to the session
                    session = saga.Session()
                    for cred in self._session.list_credentials():
                        ctx = cred._context
                        session.add_context(ctx)
                        logger.info("Added credential %s to SAGA job service." % str(cred))

                    # Create working directory if it doesn't exist and copy
                    # the agent bootstrap script into it. 
                    #
                    # We create a new sub-driectory for each agent. each 
                    # agent will bootstrap its own virtual environment in this 
                    # directory.
                    #
                    fs = saga.Url(resource_cfg['filesystem'])
                    if pilot_description['description'].working_directory is None:
                        raise exceptions.BadParameter("Working directory not defined.")
                    else:

                        if "valid_roots" in resource_cfg:
                            is_valid = False
                            for vp in resource_cfg["valid_roots"]:
                                if pilot_description['description'].working_directory.startswith(vp):
                                    is_valid = True
                            if is_valid is False:
                                raise exceptions.BadParameter("Working directory for resource '%s' defined as '%s' but needs to be rooted in %s " % (resource_key, pilot_description['description'].working_directory, resource_cfg["valid_roots"]))

                        fs.path += pilot_description['description'].working_directory

                    agent_dir_url = saga.Url("%s/pilot-%s/" \
                        % (str(fs), str(pilot_id)))

                    agent_dir = saga.filesystem.Directory(agent_dir_url, 
                        saga.filesystem.CREATE_PARENTS)
                    pilot_description_dict[pilot_id]['info']['log'].append("Created agent directory '%s'" % str(agent_dir_url))
                    logger.debug("Created agent directory '%s'" % str(agent_dir_url))

                    # Copy the bootstrap shell script
                    # This works for installed versions of saga-pilot
                    bs_script = which('bootstrap-and-run-agent')
                    if bs_script is None:
                        bs_script = os.path.abspath("%s/../../bin/bootstrap-and-run-agent" % os.path.dirname(os.path.abspath(__file__)))
                    # This works for non-installed versions (i.e., python setup.py test)
                    bs_script_url = saga.Url("file://localhost/%s" % bs_script) 

                    bs_script = saga.filesystem.File(bs_script_url)
                    bs_script.copy(agent_dir_url)
                    pilot_description_dict[pilot_id]['info']['log'].append("Copied '%s' script to agent directory" % bs_script_url)
                    logger.debug("Copied '%s' script to agent directory" % bs_script_url)

                    # Copy the agent script
                    cwd = os.path.dirname(os.path.abspath(__file__))
                    agent_path = os.path.abspath("%s/agent/sagapilot-agent.py" % cwd)
                    agent_script_url = saga.Url("file://localhost/%s" % agent_path) 
                    agent_script = saga.filesystem.File(agent_script_url)
                    agent_script.copy(agent_dir_url)
                    pilot_description_dict[pilot_id]['info']['log'].append("Copied '%s' script to agent directory" % agent_script_url)
                    logger.debug("Copied '%s' script to agent directory '%s'" % (agent_script_url, agent_dir_url))

                    # extract the required connection parameters and uids
                    # for the agent:
                    database_host = self._session._database_url.split("://")[1]
                    database_name = self._session._database_name
                    session_uid  = self._session.uid

                    # now that the script is in place and we know where it is,
                    # we can launch the agent
                    js = saga.job.Service(resource_cfg['URL'], session=session)

                    jd = saga.job.Description()
                    jd.working_directory = agent_dir_url.path
                    jd.executable        = "./bootstrap-and-run-agent"
                    jd.arguments         = ["-r", database_host,  # database host (+ port)
                                            "-d", database_name,  # database name
                                            "-s", session_uid,    # session uid
                                            "-p", str(pilot_id),  # pilot uid
                                            "-t", run_time,       # agent runtime in minutes
                                            "-c", number_cores,   # number of cores
                                            "-C"]                 # clean up by default

                    if 'task_launch_mode' in resource_cfg:
                        jd.arguments.extend(["-l", resource_cfg['task_launch_mode']])

                    # process the 'queue' attribute
                    if queue is not None:
                        jd.queue = queue
                    elif 'default_queue' in resource_cfg:
                        jd.queue = resource_cfg['default_queue']

                    # if resource config defines 'pre_bootstrap' commands,
                    # we add those to the argument list
                    if 'pre_bootstrap' in resource_cfg:
                        for command in resource_cfg['pre_bootstrap']:
                            jd.arguments.append("-e \"%s\"" % command)

                    # if resourc configuration defines a custom 'python_interpreter',
                    # we add it to the argument list
                    if 'python_interpreter' in resource_cfg:
                        jd.arguments.append("-i %s" % resource_cfg['python_interpreter'])

                    jd.output            = "STDOUT"
                    jd.error             = "STDERR"
                    jd.total_cpu_count   = number_cores
                    jd.wall_time_limit    = run_time

                    pilotjob = js.create_job(jd)
                    pilotjob.run()

                    pilotjob_id = pilotjob.id

                    # clean up / close all saga objects
                    js.close()
                    agent_dir.close()
                    agent_script.close()
                    bs_script.close()

                    # at this point, submission has succeeded. we can update
                    #   * the state to 'PENDING'
                    #   * the submission time
                    #   * the log
                    pilot_description_dict[pilot_id]['info']['state'] = states.PENDING
                    pilot_description_dict[pilot_id]['info']['log'].append("Pilot Job successfully submitted with JobID '%s'" % pilotjob_id)

                    # keep the actual submission URL around for pilot inspection
                    #pilot_description['description']['resource'] = resource_cfg['URL']

                except saga.SagaException, se: 
                    # at this point, submission has failed. we update the 
                    # agent status accordingly
                    error_msg = "Pilot Job submission failed: '%s'" % str(se)
                    pilot_description_dict[pilot_id]['info']['state'] = states.FAILED
                    pilot_description_dict[pilot_id]['info']['log'].append(error_msg)
                    raise exceptions.BadParameter(error_msg)

            # Set submission date and reate a pilot object, regardless whether 
            # submission has failed or not.                 
            pilot_description_dict[pilot_id]['info']['submitted'] = datetime.datetime.utcnow()

            pilot = ComputePilot._create(
                pilot_id=str(pilot_id),
                pilot_description=pilot_description['description'], 
                pilot_manager_obj=self)

            pilots.append(pilot)

        # bulk-create database entries
        self._DB.insert_pilots(pilot_manager_uid=self._uid, 
            pilot_descriptions=pilot_description_dict)

        # implicit return value conversion
        if len(pilots) == 1:
            return pilots[0]
        else:
            return pilots

    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the unique identifiers of all :class:`sagapilot.ComputePilot` 
        instances associated with this PilotManager

        **Returns:**

            * A list of :class:`sagapilot.ComputePilot` uids [`string`].

        **Raises:**

            * :class:`sagapilot.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        return self._DB.list_pilot_uids(self._uid)

    # --------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_ids=None):
        """Returns one or more :class:`sagapilot.ComputePilot` instances.

        **Arguments:**

            * **pilot_uids** [`list of strings`]: If pilot_uids is set, 
              only the Pilots with  the specified uids are returned. If 
              pilot_uids is `None`, all Pilots are returned.

        **Returns:**

            * A list of :class:`sagapilot.ComputePilot` objects 
              [`list of :class:`sagapilot.ComputePilot`].

        **Raises:**

            * :class:`sagapilot.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        pilots = ComputePilot._get(pilot_ids=pilot_ids, pilot_manager_obj=self)
        return pilots

    # --------------------------------------------------------------------------
    #
    def wait_pilots(self, pilot_ids=None, 
        state=[states.DONE, states.FAILED, states.CANCELED], timeout=-1.0):
        """Returns when one or more :class:`sagapilot.ComputePilots` reach a 
        specific state or when an optional timeout is reached.

        If `pilot_uids` is `None`, `wait_pilots` returns when **all** Pilots
        reach the state defined in `state`. 

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`] 
              If pilot_uids is set, only the Pilots with the specified uids are
              considered. If pilot_uids is `None` (default), all Pilots are 
              considered.

            * **state** [`list of strings`]
              The state(s) that Pilots have to reach in order for the call
              to return. 

              By default `wait_pilots` waits for the Pilots to reach 
              a **terminal** state, which can be one of the following:

              * :data:`sagapilot.DONE`
              * :data:`sagapilot.FAILED`
              * :data:`sagapilot.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless 
              whether the Pilots have reached the desired state or not. 
              The default value **-1.0** never times out.

        **Raises:**

            * :class:`sagapilot.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if not isinstance (state, list):
            state = [state]

        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        start_wait = time.time()
        all_done   = False
        return_states = []

        while all_done is False:

            all_done = True

            pilots_json = self._DB.get_pilots(pilot_manager_id=self.uid)
            for pilot in pilots_json:
                if pilot['info']['state'] not in state:
                    all_done = False
                    break # leave for loop
                else:
                    return_states.append(pilot['info']['state'])

            # check timeout
            if (None != timeout) and (timeout <= (time.time () - start_wait)):
                break

            # wait a bit
            time.sleep(1)

        # done waiting
        return return_states

    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, pilot_ids=None):
        """Cancels one or more Pilots. 

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`] 
              If pilot_uids is set, only the Pilots with the specified uids are
              canceled. If pilot_uids is `None`, all Pilots are canceled.

        **Raises:**

            * :class:`sagapilot.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        # now we can send a 'cancel' command to the pilots.
        self._DB.signal_pilots(pilot_manager_id=self._uid, 
            pilot_ids=pilot_ids, cmd="CANCEL")
