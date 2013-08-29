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
  the job instance. It corresponds to the BES state Canceled. This state is final.

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
CDS = Compute Data Service
CU = Compute Unit
CUD = CU Description
DU = DataUnit
DUD = DU Description


"""


# ------------------------------------------------------------------------------
# 
class ComputeUnitDescription(dict):
    """ ComputeUnitDescription.
    
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

    def __init__(self):
        """

        :rtype : ComputeUnit
        """
        pass


    def get_state(self):
        """
            Return the state of this Compute Unit.
        """
        pass


    def get_state_detail(self):
        """
            Return the backend specific status of this Compute Unit.
        """
        pass


    def list_metrics(self):
        """
            List the metrics available for this ComputeUnit.

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
        """
        pass

    def get_metric(self, metric):
        """
            Return the value for the specified metric.
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
        pass


# ------------------------------------------------------------------------------
#
class DataUnit():

    def __init__(self, data_unit_description=None, static=False):
        """
            Data Unit constructor.

            If static is True, the data is already located on the DataPilot location.
            Therefore no transfer is required and only the datastructure needs to be populated.
        """
        pass


    def wait(self):
        """
            Wait for Data Unit to become 'RUNNING'.
        """
        pass


    def list_data_unit_items(self):
        """
            List the content of the Data Unit.
        """
        pass


    def get_state(self):
        """
            Return the state of the Data Unit.
        """
        pass


    def split(self, num_chunks=None, chunk_size=1):
        """
            Split the DU unit in a set of DUs based on the number of chunks and chunk size.
            Return the set of DUs created.
        """
        pass


    def export(self, dest_uri):
        """
            Export the data of this Data Unit to the specified destination location.
        """
        pass




# ------------------------------------------------------------------------------
# 
class ComputePilotDescription(dict):
    """
        ComputePilotDescription.
    
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
    """
        ComputePilot class. This represents instances of ComputePilots.
    """

    def __init__(self, id=None):
        pass

    def get_state(self):
        """ Return state of PC. """
        pass

    def get_state_detail(self):
        """ Return implementation specific state details of PC. """
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

        """
        pass


    def __del__(self):
        pass


    def create_pilot(self, compute_pilot_description, context=None):
        """ Instantiate and return ComputePilot object """
        pass


    def get_state_detail(self): 
        """ Return implementation specific details of the PCS. """
        pass

    
    def cancel(self):        
        """ Cancel the PCS (self).

            This also cancels the ...
        """
        pass
                    

    def list_pilots(self):
        """ Return a list of ComputePilot's managed by this PCS. """
        pass


    def wait(self):
        """ Wait for all CU's under this PCS to complete. """
        pass





# ------------------------------------------------------------------------------
#
class DataPilotDescription(dict):
    """ DataPilotDescription.
        {
            'service_url': "ssh://localhost/tmp/pilotstore/",
            'size':100,

            # Affinity
            'affinity_datacenter_label',    # pilot stores sharing the same label are located in the same data center
            'affinity_machine_label',       # pilot stores sharing the same label are located on the same machine
        }
    """

    def __init__(self):
        pass



# ------------------------------------------------------------------------------
#
class DataPilot():
    """ DataPilot handle.
    """

    # Class members
    #    'id',           # Reference to this PJ
    #    'description',  # Description of PilotStore
    #    'context',      # SAGA context
    #    'resource_url', # Resource  URL
    #    'state',        # State of the PilotStore
    #    'state_detail', # Adaptor specific state of the PilotStore

    def __init__(self, description):


    def add_data_unit(self, du):
        """
            Add a Data Unit to this Data Pilot.
        """
        pass


    def wait(self):
        """
            Wait for pending data transfers.
        """
        pass


    def cancel(self):
        """ Cancel DataPilot

            Keyword arguments:
            None
        """
        pass


    def get_state(self):
        """
            Return the state of the DataPilot.
        """
        pass


    def get_state(self):
        """
            Return the backend specific state detail of the DataPilot.
        """
        pass




# ------------------------------------------------------------------------------
#
class DataPilotService():

    def __init__(self):
        pass



    def create_pilot(self, data_pilot_description):
        """
            Create a Data Pilot based on the Data Pilot Description and return a PilotData object.
        """
        pass


    def list_pilots(self):
        """
            Return a list of all Data Pilots that are under control of this DPS.
        """
        



    def wait(self):
        """
            Wait for all DPs to reach state 'RUNNING'
        """
        pass





# ------------------------------------------------------------------------------
#
class ComputeDataService():
    """ Service that brings the ComputePilot's and DataPilot's together. """

    def __init__(self):
        pass


    def cancel(self):
        """ Cancel service.
            This also cancels ...
        """
        pass


    def add_compute_pilot(self, cp):
        """ Bring the CP into the scope of this CDS.
        """
        pass


    def add_data_pilot(self, dp):
        """ Bring the DP into the scope of this CDS.
        """
        pass


    def submit_compute_unit(self, cud):
        """
            Accepts a CUD and returns a CU.
        """
        pass

    def submit_data_unit(self, dud):
        """
            Accepts a DUD and returns a DU.
        """
        pass

    def wait(self):
        """
            Wait for all the CUs and DUs handled by this ComputeDataService.
        """
        pass


    def list_compute_units(self):
        """
            List the Compute Units handled by this ComputeDataService.
        """
        pass


    def list_data_units(self):
        """
            List the Data Units handled by this ComputeDataService.
        """
        pass
