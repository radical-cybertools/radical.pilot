"""

saga-pilot API spec


Job States

* Unknown
  Not part of the SPEC...

* New
  This state identies a newly constructed job instance which has not yet run.
  This state corresponds to the BES state Pending. This state is initial.

* Running     
  The run() method has been invoked on the job, either explicitly or implicitly.
  This state corresponds to the BES state Running. This state is initial.

* Done    
  The synchronous or asynchronous operation has finished successfully. It
  corresponds to the BES state Finished. This state is final.

* Canceled    
  The asynchronous operation has been canceled, i.e. cancel() has been called on
  the job instance. It corresponds to the BES state Canceled.
  This state is final.

* Failed  
  The synchronous or asynchronous operation has finished unsuccessfully. It
  corresponds to the BES state Failed. This state is final.

* Suspended   
  Suspended identifies a job instance which has been suspended. This state
  corresponds to the BES state Suspend. 


Glossary

CP = Compute Pilot
CPD = CP Description
CPS = CP Service
DP = Data Pilot
DPD = DP Description
DPS = DP Service
CU = Compute Unit
CUD = CU Description
DU = DataUnit
DUD = DU Description
US = Unit Service


Signature Template:

    Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

"""


# ------------------------------------------------------------------------------
# 
class ComputeUnitDescription(dict):
    """ Task description to instantiate a ComputeUnit.
    
    The ComputeUnitDescription is a job/task/call description based on
    SAGA Job Description.

    It offers the application to describe a ComputeUnit in an abstract
    way that is dealt with by the Pilot-Manager.

    Class members:

        # Action description
        'executable',           # The "action" to execute
        'arguments',            # Arguments to the "action"
        'cleanup',
        'environment',          # "environment" settings for the "action"
        'interactive',
        'contact',
        'project',
        'start_time',
        'working_directory',

        # I/O
        'input',
        'error',
        'output',
        'file_transfer',

        # Parallelism
        'number_of_processes',  # Total number of processes to start
        'processes_per_host',   # Nr of processes per host
        'threads_per_process',  # Nr of threads to start per process
        'total_core_count',     # Total number of cores requested
        'spmd_variation',       # Type and startup mechanism

        # Requirements
        'candidate_hosts',
        'cpu_architecture',
        'total_physical_memory',
        'operating_system_type',
        'total_cpu_time',
        'wall_time_limit',
        'queue'

        """


# ------------------------------------------------------------------------------
#
class ComputeUnit():
    """ ComputeUnit object that allows for direct operations on CUs.

    The ComputeUnit object is obtained either through the submit_unit
    """

    def __init__(self):
        """


        """
        pass

    def get_state(self):
        """ Return the state of this Compute Unit.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def get_state_detail(self):
        """ Return the backend specific status of this Compute Unit.

            Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None


        """
        pass

    def list_metrics(self):
        """ List the metrics available for this ComputeUnit.

        For example:

            'AFTERDOWNLOAD',
            'BEFOREUPLOAD',
            'CREAM_JOBID',
            'DEFAULTSE',
            'DOWNLOAD',
            'ERRNO',
            'EXECUTION',
            'HOSTNAME',
            'SITE_NAME',
            'START',
            'STOP',
            'TOTAL',
            'UPLOAD'



            Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def get_metric(self, metric):
        """ Return the value for the specified metric.

    Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None


        """
        pass


# ------------------------------------------------------------------------------
# 
class DataUnitDescription(dict):
    """ DataUnitDescription.

        {
            'file_urls': [file1, file2, file3]        
        } 
        
        Currently, no directories supported
    """

    def __init__(self):
        """
        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None
        """
        pass


# ------------------------------------------------------------------------------
#
class DataUnit():

    def __init__(self, data_unit_description=None, static=False):
        """ Data Unit constructor.

        If static is True, the data is already located on the DataPilot
        location. Therefore no transfer is required and only the data
        structure needs to be populated.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def wait(self):
        """ Wait for Data Unit to become 'RUNNING'.

        Keyword arguments::

            obj: the watched object instance
            key: the watched attribute
            val: the new value of the watched attribute

        Return::

            None

        """
        pass

    def list_data_unit_items(self):
        """
            List the content of the Data Unit.


        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def get_state(self):
        """
            Return the state of the Data Unit.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def get_state_details(self):
        """
            Return the backend specific details of the DataUnit.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def split(self, num_of_chunks=None, size_of_chunks=None):
        """ Split the DU unit in a set of DUs based on the number of chunks
            and chunk size.

        Keyword arguments::

            num_of_chunks(int): the number of chunks to create.
            size_of_chunks(int): the size of chunks.

            Only one of the two arguments should be specified.

        Return::

            chunks[DU id]: a list of DU id's that were created.

        """
        pass

    def export(self, dest_uri):
        """ Export the data of this Data Unit to the specified destination
            location.

        Keyword argument::

            dest_uri(string): the location of where to export the data to.

        Return::

            None

        """
        pass


# ------------------------------------------------------------------------------
# 
class ComputePilotDescription(dict):
    """ Description used to instantiate a ComputePilot.

    The ComputePilotDescription is a description based on
    SAGA Job Description.

    It offers the application to describe a ComputePilot in an abstract
    way that is dealt with by the Pilot-Manager.

    Class members:

        # Action description
        'executable',           # The "action" to execute
        'arguments',            # Arguments to the "action"
        'cleanup',
        'environment',          # "environment" settings for the "action"
        'interactive',
        'contact',
        'project',
        'start_time',
        'working_directory',

        # I/O
        'input',
        'error',
        'output',
        'file_transfer',

        # Parallelism
        'number_of_processes',  # Total number of processes to start
        'processes_per_host',   # Nr of processes per host
        'threads_per_process',  # Nr of threads to start per process
        'total_core_count',     # Total number of cores requested
        'spmd_variation',       # Type and startup mechanism

        # Requirements
        'candidate_hosts',
        'cpu_architecture',
        'total_physical_memory',
        'operating_system_type',
        'total_cpu_time',
        'wall_time_limit',
        'queue'

        """
    pass


# ------------------------------------------------------------------------------
#
class ComputePilot():
    """ This represents instances of ComputePilots.

        capacity

    """

    def __init__(self, pilot_id=None):
        """ Constructor for the ComputePilot.

        Keyword argument::

            id(string): if specified, don't create a new ComputePilot but
            instantiate it based on the id.
            TODO: Not sure if this makes sense

        Return::
            cp(ComputePilot): the ComputePilot object


        """
        pass

    def get_state(self):
        """ Return state of PC.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def get_state_detail(self):
        """ Get implementation specific state details of PC.


        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None


        """
        pass

    def cancel(self):
        """

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """

    def submit_unit(self, ud):
        """ Submit a CUD and returns a CU.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None
        """
        pass


# ------------------------------------------------------------------------------
#
class ComputePilotService():
    """ ComputePilotService()

        Factory for ComputePilot instances.
    """
    
    def __init__(self):
        """
            Constructor for the ComputePilotService.
            This could take arguments to reconnect to an existing CPS.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def create_pilot(self, compute_pilot_description, context=None):
        """ Instantiate and return ComputePilot object.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None


        """
        pass

    def get_state_detail(self):
        """ Return implementation specific details of the PCS.


        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def cancel(self):
        """ Cancel the PCS (self).

            This also cancels the ...


        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def list_pilots(self):
        """ Return a list of ComputePilot's managed by this PCS.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def wait(self):
        """ Wait for all CU's under this PCS to complete.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass


# ------------------------------------------------------------------------------
#
class DataPilotDescription(dict):
    """ DataPilotDescription.
    {
        'service_url': "ssh://localhost/tmp/pilotstore/",
        'size':100,

        # Affinity
        # pilot stores sharing the same label are located in the same datacenter
        'affinity_datacenter_label',
        # pilot stores sharing the same label are located on the same machine
        'affinity_machine_label',
        }
    """

    def __init__(self):
        """
        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None
        """
        pass


# ------------------------------------------------------------------------------
#
class DataPilot():
    """ DataPilot handle.

    capacity

    # Class members
    #    'id',           # Reference to this PJ
    #    'description',  # Description of PilotStore
    #    'context',      # SAGA context
    #    'resource_url', # Resource  URL
    #    'state',        # State of the PilotStore
    #    'state_detail', # Adaptor specific state of the PilotStore

    """

    def __init__(self):
        """

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def add_data_unit(self, du):
        """ Add a Data Unit to this Data Pilot.

        This brings

        Keyword argument(s)::

            du(DataUnit): description

        Return::

            None

        """
        pass

    def wait(self):
        """
            Wait for pending data transfers.

        Keyword argument(s)::

        name(type): description

        Return::

        name(type): description
        or
        None

        """
        pass

    def cancel(self):
        """ Cancel DataPilot


        Keyword argument(s)::

        name(type): description

        Return::

        name(type): description
        or
        None

        """
        pass

    def get_state(self):
        """
            Return the state of the DataPilot.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass

    def get_state_detail(self):
        """
            Return the backend specific state detail of the DataPilot.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

        """
        pass


# ------------------------------------------------------------------------------
#
class DataPilotService():

    def __init__(self):
        """
        """
        pass

    def create_pilot(self, data_pilot_description):
        """ Create a Data Pilot based on the Data Pilot Description and return
            a PilotData object.

        """
        pass

    def list_pilots(self):
        """ Return a list of all Data Pilots that are under control of this DPS.

        """
        pass

    def wait(self):
        """ Wait for all DPs to reach state 'RUNNING', i.e. have finished all
            transfers.


        """
        pass


# ------------------------------------------------------------------------------
#
class UnitService():
    """ Service that brings the ComputePilot's and DataPilot's together.

    """

    def __init__(self):
        """ UnitService constructor.

        The instantiation of the Unit Service object could possibly be
        done to re-connect to a persistently running Unit Service.
        The constructor would need accommodate that.

        Keyword argument::

            None

        Return::

            None

        """
        pass

    def cancel(self):
        """ Cancel this Unit Service.

        TODO: Would this cancel assigned Units too?


        Keyword argument::

            None

        Return::

            None

        """
        pass

    def add_pilot(self, pilot):
        """ Bring a Pilot (and the resources its represents into the scope of
            the US.

        Keyword argument(s)::

            pilot(ComputePilot): A ComputePilot
            or
            pilot(DataPilot):

        Return::

            None

        """
        pass

    def submit_unit(self, ud):
        """
            Accepts a CUD or DUD and returns a CU or DU.

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None
        """
        pass

    def wait(self):
        """
            Wait for all the CUs and DUs handled by this UnitService.
        """
        pass

    def cancel_unit(self, unit_id):
        """ Cancel the specified Compute Unit or Data Unit by its ID.

        Keyword argument(s)::

            id(ComputeUnit): The ComputeUnit to cancel.
            or
            id(DataUnit): The DataUnit to cancel.

         Return::

            None

        """

    def list_units(self, compute=True, data=True):
        """
            List the Units handled by this UnitService.


        Keyword argument(s)::

            compute(bool): Enable the listing of Compute Units (default=True)
            data(bool): Enable the listing Data Units (default=True)

        Return::

            name(type): description
            or
            None

        """
        pass

    def get_unit(self, unit_id):
        """ Get a DU or CU based on its id.

        Keyword argument::

            id(string): The ID of the unit we want to acquire an instance of.

        Return::

            unit(ComputeUnit): A ComputeUnit object.
            or
            unit(DataUnit): A DataUnit object.

        """
        pass

    def list_pilots(self, compute=True, data=True):
        """ Return a list Pilot IDs of specified type of Pilots that are
        assigned to this UnitService.

        Keyword arguments::

            compute(bool): Enable the listing of Compute Pilots (default=True)
            data(bool): Enable the listing Data Pilots (default=True)

        Return::

            pilots([pilot IDs]): A list of Pilot IDs
            or
            None: If there are no Pilots (of the requested type)

        """
        pass

    def get_pilot(self, pilot_id):
        """ Get a DP or CP instance based on its ID.

        This method is required as based on the ID only we don't know which
        Pilot Service a Pilot belongs to.

        Keyword argument::

            id(string): The ID of the Pilot we want to acquire an instance of.

        Return::

            pilot(ComputePilot): A ComputePilot object.
            or
            pilot(DataPilot): A DataPilot object.

        """
        pass