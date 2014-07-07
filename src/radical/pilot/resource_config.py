__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils

import saga.attributes  as attributes
from radical.pilot.exceptions import * 

# -----------------------------------------------------------------------------
# Attribute description keys
NAME                        = 'name'
REMOTE_JOB_MANAGER_ENDPOINT = 'remote_job_manager_endpoint'
REMOTE_FILESYSTEM_ENDPOINT  = 'remote_filesystem_endpoint'
LOCAL_JOB_MANAGER_ENDPOINT  = 'local_job_manager_endpoint'
LOCAL_FILESYSTEM_ENDPOINT   = 'local_filesystem_endpoint'
DEFAULT_QUEUE               = 'default_queue'
SPMD_VARIATION              = 'spmd_variation'
PYTHON_INTERPRETER          = 'python_interpreter'
PRE_BOOTSTRAP               = 'pre_bootstrap'
VALID_ROOTS                 = 'valid_roots'
BOOTSTRAPPER                = 'bootstrapper'
PILOT_AGENT                 = 'pilot_agent'
PILOT_AGENT_WORKER          = 'pilot_agent_worker'
GLOBAL_VIRTENV              = 'global_virtenv'
LRMS                        = 'lrms'
TASK_LAUNCH_METHOD          = 'task_launch_method'
MPI_LAUNCH_METHOD           = 'mpi_launch_method'
FORWARD_TUNNEL_ENDPOINT     = 'forward_tunnel_endpoint'
AGENT_MONGODB_ENDPOINT      = 'agent_mongodb_endpoint'
DEFAULT_REMOTE_WORKDIR      = 'default_remote_workdir'


VALID_KEYS = [NAME, LOCAL_JOB_MANAGER_ENDPOINT, LOCAL_FILESYSTEM_ENDPOINT,
              REMOTE_JOB_MANAGER_ENDPOINT, REMOTE_FILESYSTEM_ENDPOINT,
              DEFAULT_QUEUE, SPMD_VARIATION, PYTHON_INTERPRETER, PRE_BOOTSTRAP, 
              VALID_ROOTS, BOOTSTRAPPER, PILOT_AGENT, PILOT_AGENT_WORKER,
              GLOBAL_VIRTENV, LRMS, TASK_LAUNCH_METHOD,
              MPI_LAUNCH_METHOD, FORWARD_TUNNEL_ENDPOINT, AGENT_MONGODB_ENDPOINT,
              DEFAULT_REMOTE_WORKDIR]

# -----------------------------------------------------------------------------
#
class ResourceConfig(attributes.Attributes):
    """A ResourceConfig object describes the internal configuration parameters 
    of a compute and / or storage resource. It can be passed to a 
    :class:`radical.pilot.PilotManager` to override or extend an existing 
    configuration set. 

    **Example**::

          rc = radical.pilot.ResourceConfig()
          rc.name                 = "archer_local"
          rc.job_manager_endpoint = "fork://localhost"
          rc.filesystem_endpoint  = "sftp://localhost"
          rc.default_queue        = None
          rc.spmd_variation       = None
          rc.python_interpreter   = "/work/y07/y07/cse/python/2.7.6/bin/python"
          rc.pre_bootstrap        = "module load mpi"
          rc.valid_roots          = ["/home", "/work"]
          rc.bootstrapper         = "cray_bootstrapper.sh"

          pmgr = radical.pilot.PilotManager(session=session)
          pmgr.add_resource_config(rc)

          // [...]

          pd = radical.pilot.ComputePilotDescription()
          pd.resource = "archer_local"

          // [...]

          pmgr.submit_pilots(pd)

    .. data:: name

       [Type: `string`] [**`mandatory`**] A unique name for this configuration. 

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

    .. data:: global_virtenv

       [Type: `string`] [optional] TODO

    .. data:: lrms

       [Type: `string`] [optional] TODO

    """

    # -------------------------------------------------------------------------
    #
    @classmethod 
    def from_file(cls, filename, entry_name=None):
      """Reads a resource configuration JSON file from the URL provided and 
         returns a list of one or more ResourceConfig objects.
      """
      rcfgs = []

      try:
          rcf_dict = radical.utils.read_json(filename)

          for name, cfg in rcf_dict.iteritems():
              cls = ResourceConfig()
              cls.name = name

              for key in cfg:
                  if key not in VALID_KEYS:
                      msg = "Unknown key '%s' in file '%s'." % (key, str(filename))
                      raise BadParameter(msg=msg)

              for key in VALID_KEYS:
                  if key == NAME:
                      continue
                  if key in cfg:
                      cls[key] = cfg[key]
                  else:
                      cls[key] = None

                  rcfgs.append(cls)

      except ValueError, err:
          raise BadParameter("Couldn't parse resource configuration file '%s': %s." % (filename, str(err)))

      return rcfgs


    # -------------------------------------------------------------------------
    #
    def __init__(self, seeding_dict=None):
        """Optionally take a seeding dict to populate the values.
        """

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register(NAME,                        None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(REMOTE_JOB_MANAGER_ENDPOINT, None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(REMOTE_FILESYSTEM_ENDPOINT,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LOCAL_JOB_MANAGER_ENDPOINT,  None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LOCAL_FILESYSTEM_ENDPOINT,   None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(DEFAULT_QUEUE,               None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(SPMD_VARIATION,              None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PYTHON_INTERPRETER,          None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PRE_BOOTSTRAP,               None, attributes.STRING, attributes.VECTOR, attributes.WRITEABLE)
        self._attributes_register(VALID_ROOTS,                 None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(BOOTSTRAPPER,                None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PILOT_AGENT,                 None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(PILOT_AGENT_WORKER,          None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(GLOBAL_VIRTENV,              None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(LRMS,                        None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(TASK_LAUNCH_METHOD,          None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(MPI_LAUNCH_METHOD,           None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(FORWARD_TUNNEL_ENDPOINT,     None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(AGENT_MONGODB_ENDPOINT,      None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)
        self._attributes_register(DEFAULT_REMOTE_WORKDIR,      None, attributes.STRING, attributes.SCALAR, attributes.WRITEABLE)

        # Parse the seeding dict if it is provided
        if seeding_dict is not None:
            try:
                for key in seeding_dict:
                    self.set_attribute(key, seeding_dict[key])

            except ValueError, err:
                raise BadParameter("Couldn't parse seeding dict: %s." % str(err))



    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())
