__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import radical.utils

import saga.attributes  as attributes
from radical.pilot.exceptions import * 

# -----------------------------------------------------------------------------
# Attribute description keys
LABEL                       = 'label'
JOB_MANAGER_ENDPOINT        = 'job_manager_endpoint'
FILESYSTEM_ENDPOINT         = 'filesystem_endpoint'
DEFAULT_QUEUE               = 'default_queue'
SPMD_VARIATION              = 'spmd_variation'
PYTHON_INTERPRETER          = 'python_interpreter'
PRE_BOOTSTRAP               = 'pre_bootstrap'
VALID_ROOTS                 = 'valid_roots'
BOOTSTRAPPER                = 'bootstrapper'
PILOT_AGENT                 = 'pilot_agent'
PILOT_AGENT_WORKER          = 'pilot_agent_worker'
GLOBAL_VIRTENV              = 'global_virtenv'
VIRTENV                     = 'virtenv'
VIRTENV_MODE                = 'private'
LRMS                        = 'lrms'
TASK_LAUNCH_METHOD          = 'task_launch_method'
MPI_LAUNCH_METHOD           = 'mpi_launch_method'
AGENT_SCHEDULER             = 'agent_scheduler'
FORWARD_TUNNEL_ENDPOINT     = 'forward_tunnel_endpoint'
TUNNEL_BIND_DEVICE          = "tunnel_bind_device"
AGENT_MONGODB_ENDPOINT      = 'agent_mongodb_endpoint'
DEFAULT_REMOTE_WORKDIR      = 'default_remote_workdir'
DESCRIPTION                 = 'description'
NOTES                       = 'notes'
SCHEMAS                     = 'schemas'


VALID_KEYS = [JOB_MANAGER_ENDPOINT, FILESYSTEM_ENDPOINT, SCHEMAS, TUNNEL_BIND_DEVICE,
              DEFAULT_QUEUE, SPMD_VARIATION, PYTHON_INTERPRETER, PRE_BOOTSTRAP, 
              VALID_ROOTS, BOOTSTRAPPER, PILOT_AGENT, PILOT_AGENT_WORKER,
              VIRTENV, VIRTENV_MODE, LRMS, TASK_LAUNCH_METHOD, AGENT_SCHEDULER,
              MPI_LAUNCH_METHOD, FORWARD_TUNNEL_ENDPOINT, AGENT_MONGODB_ENDPOINT,
              DEFAULT_REMOTE_WORKDIR, NOTES, DESCRIPTION]

# -----------------------------------------------------------------------------
#
class ResourceConfig(attributes.Attributes):
    """A ResourceConfig object describes the internal configuration parameters 
    of a compute and / or storage resource. It can be passed to a 
    :class:`radical.pilot.PilotManager` to override or extend an existing 
    configuration set. 

    **Example**::

          rc = radical.pilot.ResourceConfig(label='epsrc.archer')
          rc.job_manager_endpoint = "ssh://23.23.23.23/"
          rc.filesystem_endpoint  = "sftp://23.23.23.23"
          rc.default_queue        = "batch"
          rc.python_interpreter   = "/opt/python/2.7.6/bin/python"
          rc.pre_bootstrap        = "module load mpi"
          rc.valid_roots          = ["/home", "/work"]
          rc.bootstrapper         = "default_bootstrapper.sh"

          pmgr = radical.pilot.PilotManager(session=session)
          pmgr.add_resource_config(rc)

          # [...]

          pd = radical.pilot.ComputePilotDescription()
          pd.resource = "epsrc.archer"

          # [...]

          pmgr.submit_pilots(pd)

    .. parameter:: label

       [Type: `string`] [**`mandatory`**] A unique label for this configuration. 

    .. data:: remote_job_manager_endpoint

       [Type: `string`] [optional] TODO

    .. data:: remote_filesystem_endpoint

       [Type: `string`] [optional] TODO

    .. data:: local_job_manager_endpoint

       [Type: `string`] [optional] TODO

    .. data:: local_filesystem_endpoint

       [Type: `string`] [optional] TODO

    .. data:: default_queue

       [Type: `string`] [optional] TODO

    .. data:: spmd_variation

       [Type: `string`] [optional] TODO

    .. data:: python_interpreter

       [Type: `string`] [optional] TODO

    .. data:: pre_bootstrap

       [Type: `string`] [optional] TODO

    .. data:: valid_roots

       [Type: `string`] [optional] TODO

    .. data:: bootstrapper

       [Type: `string`] [optional] TODO

    .. data:: pilot_agent

       [Type: `string`] [optional] TODO

    .. data:: virtenv

       [Type: `string`] [optional] TODO

    .. data:: virtenv_mode

       [Type: `string`] [optional] TODO

    .. data:: lrms

       [Type: `string`] [optional] TODO

    .. data:: agent_scheduler

       [Type: `string`] [optional] TODO

    """

    # -------------------------------------------------------------------------
    #
    @staticmethod 
    def from_file(filename, entry_name=None):
      """Reads a resource configuration JSON file from the URL provided and 
         returns a list of one or more ResourceConfig objects.
      """
      rcfgs = dict()

      try:
          rcf_dict = radical.utils.read_json_str (filename)
          rcf_name = str(os.path.basename (filename))

          if  rcf_name.endswith ('.json'):
              rcf_name = rcf_name[0:-5]

          if  'aliases' in rcf_dict:
              # return empty list
              return []

          for res_name, cfg in rcf_dict.iteritems():

              # create config from resource section
              label = "%s.%s" % (rcf_name, res_name)
              rcfgs[label] = ResourceConfig(label, cfg)

      except ValueError, err:
          raise BadParameter("Couldn't parse resource configuration file '%s': %s." % (filename, str(err)))

      return rcfgs


    # -------------------------------------------------------------------------
    #
    def __init__(self, label, seeding_dict=None):
        """Optionally take a seeding dict to populate the values.
        """

        if not seeding_dict:
            seeding_dict = dict()

        # initialize attributes
        attributes.Attributes.__init__(self, seeding_dict)

        # set attribute interface properties
        self._attributes_extensible  (True)
        self._attributes_camelcasing (True)

        self._attributes_register(LABEL,                  label, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(JOB_MANAGER_ENDPOINT,    None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(FILESYSTEM_ENDPOINT,     None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(DEFAULT_QUEUE,           None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(SPMD_VARIATION,          None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PYTHON_INTERPRETER,      None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PRE_BOOTSTRAP,           None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(VALID_ROOTS,             None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(BOOTSTRAPPER,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PILOT_AGENT,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PILOT_AGENT_WORKER,      None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(VIRTENV,                 None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(VIRTENV_MODE,            None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LRMS,                    None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(AGENT_SCHEDULER,         None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(TASK_LAUNCH_METHOD,      None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(MPI_LAUNCH_METHOD,       None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(FORWARD_TUNNEL_ENDPOINT, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(TUNNEL_BIND_DEVICE,      None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(AGENT_MONGODB_ENDPOINT,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(DEFAULT_REMOTE_WORKDIR,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(DESCRIPTION,             None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(NOTES,                   None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())
