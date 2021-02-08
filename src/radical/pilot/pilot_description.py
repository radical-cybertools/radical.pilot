
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru


# ------------------------------------------------------------------------------
# Attribute description keys
#
UID               = 'uid'
RESOURCE          = 'resource'
ACCESS_SCHEMA     = 'access_schema'
QUEUE             = 'queue'
JOB_NAME          = 'job_name'
PROJECT           = 'project'
CANDIDATE_HOSTS   = 'candidate_hosts'
SANDBOX           = 'sandbox'
OUTPUT            = 'output'
ERROR             = 'error'
RUNTIME           = 'runtime'
APP_COMM          = 'app_comm'
CLEANUP           = 'cleanup'
EXIT_ON_ERROR     = 'exit_on_error'

LAYOUT            = 'layout'

NODES             = 'nodes'
CORES             = 'cores'
GPUS              = 'gpus'
MEMORY            = 'memory'

INPUT_STAGING     = 'input_staging'
OUTPUT_STAGING    = 'output_staging'

PREPARE_ENV       = 'prepare_env'


# ------------------------------------------------------------------------------
#
class PilotDescription(ru.Description):
    """
    A PilotDescription object describes the requirements and properties
    of a :class:`radical.pilot.Pilot` and is passed as a parameter to
    :meth:`radical.pilot.PilotManager.submit_pilots` to instantiate and run
    a new pilot.

    .. note:: A PilotDescription **MUST** define at least
              :data:`resource`, :data:`cores` and :data:`runtime`.

    **Example**::

          pm = radical.pilot.PilotManager(session=s)
          pd = radical.pilot.PilotDescription()
          pd.resource = "local.localhost"
          pd.cores    = 16
          pd.runtime  = 5 # minutes

          pilot = pm.submit_pilots(pd)

    .. data:: uid

       A unique ID for the compute pilot (`string`). This attribute is optional,
       a unique ID will be assigned by RP if the field is not set.

       default: `None`


    .. data:: resource

       [Type: `string`] [**`mandatory`**] The key of a
       :ref:`chapter_machconf` entry.
       If the key exists, the machine-specifc configuration is loaded from the
       configuration once the PilotDescription is passed to
       :meth:`radical.pilot.PilotManager.submit_pilots`. If the key doesn't exist,
       a :class:`radical.pilot.pilotException` is thrown.

    .. data:: access_schema

       [Type: `string`] [**`optional`**] The key of an access mechanism to use.
       The valid access mechanism are defined in the resource configurations,
       see :ref:`chapter_machconf`.  The first one defined there is used by
       default, if no other is specified.

    .. data:: runtime

       [Type: `int`] [**mandatory**] The maximum run time (wall-clock time) in
       **minutes** of the Pilot.

    .. data:: sandbox

       [Type: `string`] [optional] The working ("sandbox") directory  of the
       Pilot agent. This parameter is optional. If not set, it defaults
       to `radical.pilot.sandox` in your home or login directory.

       .. warning:: If you define a Pilot on an HPC cluster and you want
                 to set `sandbox` manually, make sure that it points to a
                 directory on a shared filesystem that can be reached from all
                 compute nodes.

    .. data:: cores

       [Type: `int`] [**mandatory**] The number of cores the pilot should
       allocate on the target resource.

       NOTE: for local pilots, you can set a number larger than the physical
       machine limit when setting `RADICAL_PILOT_PROFILE` in your environment.

    .. data:: gpus

       [Type: `int`] [optional] The number of gpus the pilot should allocate
       on the target resource.

    .. data:: memory

       [Type: `int`] [optional] The total amount of physical memory the pilot
       (and related to it job) requires. This parameter translates into
       `TotalPhysicalMemory` at `radical.saga.job.Description`.

    .. data:: queue

       [Type: `string`] [optional] The name of the job queue the pilot should
       get submitted to . If `queue` is defined in the resource configuration
       (:data:`resource`) defining `queue` will override it explicitly.

    .. data:: project

       [Type: `string`] [optional] The name of the project / allocation to
       charge for used CPU time. If `project` is defined in the machine
       configuration (:data:`resource`), defining `project` will
       override it explicitly.

    .. data:: candidate_hosts

       [Type: `list`] [optional] The list of names of hosts where this pilot
       is allowed to start on.

    .. data: app_comm

       [Type: `list`] [optional] The list of names is interpreted as
       communication channels to start within the pilot agent, for the purpose
       of application communication, ie., that tasks running on that pilot are
       able to use those channels to communicate amongst each other.

       The names are expected to end in `_queue` or `_pubsub`, indicating the
       type of channel to create.  Once created, tasks will find environment
       variables of the name `RP_%s_IN` and `RP_%s_OUT`, where `%s` is replaced
       with the given channel name (uppercased), and `IN/OUT` indicate the
       respective endpoint addresses for the created channels

    .. data:: cleanup

       [Type: `bool`] [optional] If cleanup is set to True, the pilot will
       delete its entire sandbox upon termination. This includes individual
       ComputeUnit sandboxes and all generated output data. Only log files will
       remain in the sandbox directory.

    .. data:: layout

       [Type: `str` or `dict`] [optional] Point to a json file or an explicit
       (dict) description of the pilot layout: number and size partitions and
       their configuration.


    .. data:: prepare_env

       [Type: `dict`] [optional]: specification of task environments to be
       prepared by the pilot.  The parameter is expected to be a dictionary of
       the form:

           {
              'env_1' : {'type'   : 'virtualenv',
                         'version': '3.6',
                         'setup'  : ['radical.pilot==1.0', 'pandas']},
              'env_2' : {'type'   : 'conda',
                         'version': '3.8',
                         'setup'  : ['numpy']},
              ...

           }

       where the `type` specifies the environment type, `version` specifies the
       env version to deploy, and `setup` specifies  how the environment is to
       be prepared.

       At this point, the implementation only accepts `virtualenv` type
       requests, where `version` specifies the Python version to use, and
       `setup` is expected to be a list of module specifiers which need to be
       installed into the environment.

    """

    _schema = {
        UID             : str        ,
        RESOURCE        : str        ,
        ACCESS_SCHEMA   : str        ,
        RUNTIME         : int        ,
        APP_COMM        : [str]      ,
        SANDBOX         : str        ,
        CORES           : int        ,
        GPUS            : int        ,
        MEMORY          : int        ,
        QUEUE           : str        ,
        JOB_NAME        : str        ,
        PROJECT         : str        ,
        CLEANUP         : bool       ,
        CANDIDATE_HOSTS : [str]      ,
        EXIT_ON_ERROR   : bool       ,
        INPUT_STAGING   : [str]      ,
        OUTPUT_STAGING  : [str]      ,
        PREPARE_ENV     : {str: None},
        LAYOUT          : None       ,
    }

    _defaults = {
        UID             : None       ,
        RESOURCE        : None       ,
        ACCESS_SCHEMA   : None       ,
        RUNTIME         : 10         ,
        APP_COMM        : []         ,
        SANDBOX         : None       ,
        CORES           : 1          ,
        GPUS            : 0          ,
        MEMORY          : 0          ,
        QUEUE           : None       ,
        JOB_NAME        : None       ,
        PROJECT         : None       ,
        CLEANUP         : False      ,
        CANDIDATE_HOSTS : []         ,
        EXIT_ON_ERROR   : True       ,
        INPUT_STAGING   : []         ,
        OUTPUT_STAGING  : []         ,
        PREPARE_ENV     : {}         ,
        LAYOUT          : 'default'  ,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        ru.Description.__init__(self, from_dict=PilotDescription._defaults)

        if from_dict:
            self.update(from_dict)


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        if not self.get('resource'):
            raise ValueError("Pilot description needs 'resource'")





# ------------------------------------------------------------------------------

