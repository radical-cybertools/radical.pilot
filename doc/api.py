"""
Notes AM:
---------

  - The current state of the API does not allow for explicit early binding, i.e.
    you cannot define a pilot, add it to a queue, and assign CUs to it, w/o any
    of this actually being executed. [ discussed: this is a feature, and
    will be solved on the troy layer ]

    Related: it is not defined in what state a pilot needs to be when being
    added to a queue.  Probably any non-final state.  Allowing to add a pilot in
    'new' state would cater to the early binding use case, but that implies
    a different way of handling pilot life time on PS level.

    We are also missing direct submission, don't we? [added in the meanwhile]

    I tried to adjust the state models to address these issues (the states were
    placeholders anyways), but the call sequences need to be checked, too.


  - Inspection on all entities is largely missing.

  - need means to expose bulk ops

  - async op model needs to be applied (borrow from saga-python?



SAGA-Pilot API spec
===================


Pilot States
------------

* Unknown
  No state information could be obtained for that pilot.

* New
  This state identifies a newly constructed pilot which is not yet submitted,
  and thus is not yet scheduled to run on any specific resource.
  This state corresponds to the BES state Pending. This state is initial.

* Pending
  This state identifies a newly constructed pilot which is not yet Running, but
  is waiting to acquire control over the target resource.
  This state corresponds to the BES state Pending.

* Running     
  The pilot has successfully acquired control over the target resource,  and can
  serve unit requests.
  This state corresponds to the BES state Running.

* Done    
  The pilot has finished, and does not utilize any resources on the target
  resource anymore.  It finished due to 'natural causes' -- for example, it
  might have reached the end of its designated lifetime.
  This state corresponds to the BES state Finished. This state is final.

* Canceled    
  The pilot has been canceled, i.e. cancel() has been called on
  the job instance. 
  This state corresponds to the BES state Canceled. This state is final.

* Failed  
  The pilot has abnormally finished -- it either met an internal error condition
  which caused it to abort, or it has been unexpectedly killed by the resource
  manager.
  This state corresponds to the BES state Failed. This state is final.



Unit States
-----------

* Unknown
  No state information could be obtained for that unit.

* New
  This state identifies a newly constructed unit which is neither assigned to
  a pilot, nor is it submitted to a UnitService.
  This state corresponds to the BES state Pending. This state is initial.

* Assigned
  This state identifies a newly constructed unit which is already assigned to
  a pilot, but not submitted to the pilot, yet -- most likely because the pilot
  is not yet running either.
  This state corresponds to the BES state Pending. This state is initial.

* Pending
  This state identifies a newly constructed unit which is not yet Running, but
  is waiting to be enacted by the Pilot.
  This state corresponds to the BES state Pending. This state is initial.

* Running     
  The pilot has successfully acquired control over the target resource,  and can
  serve unit requests.
  This state corresponds to the BES state Running.

* Done    
  The pilot has finished, and does not utilize any resources on the target
  resource anymore.  It finished due to 'natural causes' -- for example, it
  might have reached the end of its designated lifetime.
  This state corresponds to the BES state Finished. This state is final.

* Canceled    
  The pilot has been canceled, i.e. cancel() has been called on
  the job instance. 
  This state corresponds to the BES state Canceled. This state is final.

* Failed  
  The pilot has abnormally finished -- it either met an internal error condition
  which caused it to abort, or it has been unexpectedly killed by the resource
  manager.
  This state corresponds to the BES state Failed. This state is final.



Glossary
--------

CU  = Compute Unit
CUD = CU Description

DU  = Data Unit
DUD = DU Description

CP  = Compute Pilot
CPD = CP Description
CPS = CP Service

DP  = Data Pilot
DPD = DP Description
DPS = DP Service

US  = Unit Service



Signature Template:
-------------------

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


        # AM: ID is missing. [descriptions dont have id's]

        # Action description
        'executable',           # The "action" to execute
        'arguments',            # Arguments to the "action"
        'cleanup',              # AM: does not make sense for pilot systems,
                                #     IMHO
        'environment',          # "environment" settings for the "action"
        'interactive',          # AM: does not make sense for CUs, IMHO
        'contact',              # AM: is this ever used, really?  We have
                                #     monitoring...
        'project',              # AM: does that make sense?  There is no
                                #     accounting on pilot level...  What is
                                #     the error mode (as that can only be 
                                #     evaluated at runtime, if at all).
        'start_time',
        'working_directory',

        # I/O
        'input',
        'error',
        'output',
        'file_transfer',        # AM: how do those things tie into DUs?

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

        # AM: we also need simple dependencies, and the ability to mark
        # multiple CUs as 'Concurrent', etc.
    """


# ------------------------------------------------------------------------------
#
class ComputeUnit():
    """ ComputeUnit object that allows for direct operations on CUs.

    The ComputeUnit object is obtained either through the submit_unit
    """

    def __init__(self, cu_id=None):
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

            'STATE',
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


        AM: callback registration is missing.


        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def get_description(self):
        """ Returns a ComputeUnitDescription for this instance.

        """
        pass

    def get_id(self):
        """ Returns an ID (string) for this instance """
        pass

    def wait(self, timeout=-1.0, state='FINAL'):
        """
        Returns when the unit reaches the specified state, or after timeout
        seconds, whichever comes first.  Calls with timeout<0.0 will wait
        forever.
        """
        pass


# ------------------------------------------------------------------------------
# 
class DataUnitDescription(dict):
    """ DataUnitDescription.

        {
            # AM: ID is missing.

            'file_urls': [file1, file2, file3]        
        } 
        
        Currently, no directories supported.

     AM: I am still confused about the symmetry aspects to ComputeUnits.  Why
            is here no CandidateHosts, for example?  Project?  Contact?
            LifeTime?  Without those properties, there is not much resource
            management the data-pilot can do, beyond clever data staging
            / caching...

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

        AM: so, static negates early binding, right?

        AM: What is the use case for static?


        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None

    """
    pass

    def wait(self, state='RUNNING'):
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

    # AM: needs most/all methods from ComputeUnit, right?


# ------------------------------------------------------------------------------
# 
class ComputePilotDescription(dict):
    """ Description used to instantiate a ComputePilot.

    The ComputePilotDescription is a description based on
    SAGA Job Description.

    It offers the application to describe a ComputePilot in an abstract
    way that is dealt with by the Pilot-Manager.

    Class members:

        # AM: ID is missing. [ probably dont needs ID]

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

        # AM: how is working_directory relevant?  Shouldn't that be left to
        # the discretion of the pilot system?  Not sure if that notion of
        # a pwd will exist for a pilot (it should exist for a CU)...

        # interactive does not make sense.

        # AM: how is environment specified?  The env for the pilot should be
        # up to the framework -- the user does not know how env is
        # interpreted.  So, is that env for future  CUs/DUs?  That overlaps
        # with env specified there.  What happens on conflicts?  cross refs?
        # early/late binding?  Should be left out...

        # AM: exe/args should be left out -- this is up to the discretion of
        # the pilot systems.  The user cannot possibly know if this is an exe
        # in the first place...

        # I/O
        'input',
        'error',
        'output',
        'file_transfer',

        # AM: what does file_transfer mean?  Are those files presented to
        # the CUs/DUs?  That overlaps with DUs, really?  Should be left
        # out.

        # Parallelism
        'number_of_processes',  # Total number of processes to start
        'processes_per_host',   # Nr of processes per host
        'threads_per_process',  # Nr of threads to start per process
        'total_core_count',     # Total number of cores requested
        'spmd_variation',       # Type and startup mechanism

        # AM: how is spmd_variation relevant?  Also, shouldn't we just
        # specify a number of cores, and leave actual layout to the pilot
        # system?  This would otherwise make automated pilot placement very
        # hard...

        # Requirements
        'candidate_hosts',
        'cpu_architecture',
        'total_physical_memory',
        'operating_system_type',
        'total_cpu_time',
        'wall_time_limit',
        'queue'

        # AM: how is total memory specified?  Is that memory usable for CUs?
        # individually / concurrently?

        # AM: pilots don't directly consume cpu time -- wall-time should
        # suffice?


        # AM: I think pilot description should be fully reduced to
        # a description of the resource slice to be managed by the pilot,
        # w/o any details on the actual pilot startup etc.

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
        AM: do we need 'drain' on cancel?  See SAGA resource API...

        Keyword argument(s)::

        name(type): description

    Return::

        name(type): description
        or
        None
    """
    pass

    def get_id(self):
        """ Returns an ID (string) for this instance.

        """
        pass

    def get_description(self):
        """ Returns a ComputePilotDescription for this instanc.
        """
        pass

    def wait(self, timeout=-1.0, state='RUNNING'):
        """
        Returns when the pilot reaches the specified state, or after timeout
        seconds, whichever comes first.  Calls with timeout<0.0 will wait
        forever.
        """
        pass

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

        AM: why not 'submit()' ?  Create does not imply to schedule/run the
        pilot.  Is that the intent?  Then we need run() on the pilot.


        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    # AM: as discussed, this should not have state, but should have an ID for
    #     reconnect [I see a case for state, TBD]

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

    def get_state_detail(self):
        """ Return implementation specific details of the PCS. """
        pass

    def cancel(self):
        """ Cancel the PCS (self).

        This also cancels the ...

        AM: We should also be able to cancel the CPS w/o canceling the
            pilots! [I agree]

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

    # AM: why don't we have those labels on the CP?  We need to check
    # with Melissa if that is required / sufficient for expressing pilot
    # affinities (I expect they need more detail).

    # AM: also, what about affinities on CU / DU level, where are tgose
    # expressed?

    # AM: lifetime, resource information, etc.

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
            Add a Data Unit to this Data Pilot.

        """
        pass

    def add_data_unit(self, du):
        """ Add a Data Unit to this Data Pilot.

        AM: what a bout remove / list?  What does that do exactly?  When are
            data staged (if at all)?  What state needs the DU to be in?  Can
            that fail?

        This brings

        Keyword argument(s)::

            du(DataUnit): description

        Return::

            None

        """
        pass

    def wait(self):
        """ Wait for pending data transfers.

        AM: Which transfers?  Assume I have a DU which is transfered, then
        I call wait, before DU1 is finished, a DU2 gets added and needs
        transfer -- will wait return?  Isn't it better to wait on the DU
        (which is the thing which has state in the first place)?  Will it
        return or fail on failed staging?

        AM: needs a timeout for consistency


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

        AM: what happens to the DUs?  To DUs which are in use?

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

    # AM: should be fully symmetric to CPS


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

    AM: and adds some scheduling, and enacts DU/CU dependencies.

    AM: I thought again about how that will map to the Sinon queues eventually.
    Not sure what you think about it, but we could pass a queue parameter to
    most calls, like:

      add_pilot   (self, queue)
      list_pilots (self, queue)
      submit_unit (self, queue)

      
    which would also allow to create new queues -- which is something we would
    need for Troy anyway, in some form:

      create_queue (self, name, scheduler='default')
      drain_queue  (self, 
      list_queues  (self)


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

    # AM: need list_pilots(queue), and remove_pilot(queue)

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
        """ Wait for all the CUs and DUs handled by this UnitService.

        # AM: what does 'handled' mean?  All assigned to a pilot?  All
        # submitted to pilots? All DONE?  All in final state?  What happens
        # if new CUs are submitted while waiting?  What happens if  CUs
        # exist but no pilots have been added / pilots died?
        #
        # As before, I would prefer to wait on the things which have state
        # in the first place.  This wait seems convenient on a first glance,
        # but will be hard to specify/implement, and if all is done and said
        # will will only be able to cover a limited set of cases (i.e. just
        # one of the above)...

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
