"""

Discussion
----------

    AM: Inspection on all entities is largely missing.
    MS: I probably agree, we need to discuss the specifics of that.
    AGREEMENT: We'll introduce the attribute interface on: pilot, *unit, 

    AM: need means to expose bulk ops.
    MS: Agree, let's discuss a mechanism. Probably also needs ties to the
    "concurrent execution" property.

    AM: async op model needs to be applied (borrow from saga-python?
    MS: I think that model will do.

    MS: Make errors / exceptions explicit.


SAGA-Pilot API spec
===================


Pilot States
------------

* Unknown
  No state information could be obtained for that pilot.

* Pending
  This state identifies a newly constructed pilot which is not yet Running, but
  is waiting to acquire control over the target resource.
  This state corresponds to the BES state Pending.  This state is initial.

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

* Pending
  This state identifies a newly submitted unit which is not yet Running, but
  is waiting to be assigned to and enacted by a Pilot.
  This state corresponds to the BES state Pending. This state is initial.

* Running     
  The unit has successfully assigned to a pilot and is being executed by that
  pilot -- i.e., it consumes resources.
  This state corresponds to the BES state Running.

* Done
  The unit's execution has finished, and does not utilize any resources on the
  target resource anymore.  It finished due to 'natural causes' -- for example,
  it might have reached the end of its designated lifetime.
  This state corresponds to the BES state Finished. This state is final.

* Canceled    
  The unit has been canceled, i.e. cancel() has been called on it.
  This state corresponds to the BES state Canceled. This state is final.

* Failed  
  The unit has abnormally finished -- it either met an internal error condition
  which caused it to abort, or it has been unexpectedly killed by the pilot or
  unit manager.
  This state corresponds to the BES state Failed. This state is final.



Exceptions
----------

As SAGA-Pilot is obviously based on SAGA, the exceptions are derived from
SAGA's exception model, and can be extended where we see fit.


* NotImplemented
  SAGA-Pilot does not implement this method or class.

* IncorrectURL
  The given URL could not be interpreted, for example due to an incorrect
  / unknown schema. 

* BadParameter
  A given parameter is out of bound or ill formatted.

* AlreadyExists
  The entity to be created already exists.

* DoesNotExist
  An operation tried to access a non-existing entity.

* IncorrectState
  The operation is not allowed on the entity in its current state.

* PermissionDenied
  The used identity is not permitted to perform the requested operation.

* AuthorizationFailed
  The backend could not establish a valid identity.
 
* AuthenticationFailed
  The backend could not establish a valid identity.

* Timeout
  The interaction with the backend times out.

* NoSuccess
  Some other error occurred.




Glossary
--------

CU  = Compute Unit
CUD = CU Description

DU  = Data Unit
DUD = DU Description

CP  = Compute Pilot
CPD = CP Description

DP  = Data Pilot
DPD = DP Description

PS = Pilot Service
US  = Unit Service



Signature Template:
-------------------

    Keyword argument(s)::

        name(type): description.

    Return::

        name(type): description.
        or
        None

    Raises::

        name: reason.
        or
        None

"""


# ------------------------------------------------------------------------------
# 
class ComputeUnitDescription(dict):
    """Task description to instantiate a ComputeUnit.
    
    The ComputeUnitDescription is a job/task/call description based on
    SAGA Job Description.

    It offers the application to describe a ComputeUnit in an abstract
    way that is dealt with by the Pilot-Manager.

    Class members:

        # Action description
        'executable',           # The "action" to execute
        'arguments',            # Arguments to the "action"
        'cleanup',              # AM: does not make sense for pilot systems,
                                #     IMHO
                                # MS: It would instruct the agent to actively
                                # cleanup after the CU has finished?
        'environment',          # "environment" settings for the "action"
        'interactive',          # AM: does not make sense for CUs, IMHO
                                # MS: Makes as much sense for CUs as it did
                                # /does for "jobs", doesn't it?
        'contact',              # AM: is this ever used, really?  We have
                                #     monitoring...
                                # MS: You don't want email? :-) (Context,
                                # this is just a 1-2-1 copy of the SAGA JD.
                                # I'm happy to drop fields like this,
                                # but it won't hurt much either to keep it.
                                # AM: If it is here, it needs to be supported.  
                                # But it is redundant with the monitoring
                                # facilities we will have -- we can easily add
                                # an email monitoring consumer to that
                                # service...
                                #
        'project',              # AM: does that make sense?  There is no
                                #     accounting on pilot level...  What is
                                #     the error mode (as that can only be
                                #     evaluated at runtime, if at all).
                                # MS: I probably on this.
                                # AM: this statement no verb ;)
        'start_time',
        'working_directory',

        # I/O
        'input',                # stdin
        'error',                # stderr
        'output',               # stdout
        'file_transfer',        # AM: how do those things tie into DUs?
                                # MS: They don't I think, complimentary
                                # AM: do we need / want two different handles on
                                # data?
        'input_data',           # DUs for input.
        'output_data',          # DUs for output.

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
        # MS: Yes, we need to discuss the details of this.
    """


# ------------------------------------------------------------------------------
#
class ComputeUnit():
    """ComputeUnit object that allows for direct operations on CUs.

    """

    def __init__(self, cu_id=None):
        """Compute Unit constructor.

        MS: If we just have textual IDs, then we can't construct CUs using
        the ID only, as we would have no idea which US to talk too.
        Which is fine, but then we get rid of the cu_id argument here and
        "just" use the get_unit() call in the SU.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        Raises::

            None

        """
        pass

    def get_state(self):
        """Return the state of this Compute Unit.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def list_metrics(self):
        """List the metrics available for this ComputeUnit.

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
        """Return the value for the specified metric.


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
        """Returns a ComputeUnitDescription for this instance.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None


        """
        pass

    def get_id(self):
        """Returns an ID (string) for this instance.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def wait(self, timeout=1.0, state='FINAL'):
        """Returns when the unit reaches the specified state, or after timeout
        seconds, whichever comes first.

        Calls with timeout<0.0 will wait forever.

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
    """DataUnitDescription.

    {
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
    """DataUnit is a logical handle to a piece of data without explicitly
    refering to its location.


    MS: Need to think about copying & replication, i.e. at what level
    do we expose that functionality.

    """

    def __init__(self, data_unit_description=None, static=False):
        """ Data Unit constructor.

        If static is True, the data is already located on the Data Pilot
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
        """Wait for Data Unit to become 'RUNNING'.

        Keyword arguments::

            state(STATE): The state to wait for.

        Return::

            None

        """
        pass

    def list_items(self):
        """List the content of the Data Unit.


        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def get_state(self):
        """Return the state of the Data Unit.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def add_file(self, file):
        """Add file to the Data Unit.

        Keyword argument::

            file(uri): the location of the file to copy to the DU.

        Return::

            None

        """
        pass

    def remove_file(self, filename):
        """Remove file from the Data Unit.

        Keyword argument::

            filename(string): the name of the file to remove from the DU.

        Return::

            None

        """
        pass

    def export(self, dest_uri):
        """Export the data of this Data Unit to the specified destination
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
    """Description used to instantiate a ComputePilot.

    The ComputePilotDescription is a description based on
    SAGA Job Description.

    It offers the application to describe a ComputePilot in an abstract
    way that is dealt with by the Pilot-Manager.

    Class members:

    # Action description
    'cleanup',
    'environment',          # "environment" settings for the "action"

    AM: how is environment specified?  The env for the pilot should be
    up to the framework -- the user does not know how env is
    interpreted.  So, is that env for future  CUs/DUs?  That overlaps
    with env specified there.  What happens on conflicts?  cross refs?
    early/late binding?  Should be left out...
    MS: Didn't think too much about it, but I could think that it would
    pass something so that the agent runs "nicer". Also here the question is
    about how much the pilot-layer knows about the resource specifics.
    I agree that this is not for the CU's.

    'contact',
    'project',
    'start_time',
    'working_directory',

    AM: how is working_directory relevant?  Shouldn't that be left to
    the discretion of the pilot system?  Not sure if that notion of
    a pwd will exist for a pilot (it should exist for a CU)...
    MS: I'm tempted to say that the "thing" that calls saga-pilot, knows
    possibly a bit more about the resource than saga-pilot. So it might
    specify that the working directory should be different than the default?

    # I/O
    'input',                # stdin # MS: Candidate for axing?
    'error',                # stderr
    'output',               # stdout
    'file_transfer',        # File in/out staging

    AM: what does file_transfer mean?  Are those files presented to
    the CUs/DUs?  That overlaps with DUs, really?  Should be left
    out.
    MS: In the case of not using PilotData, this would be a way to make sure
    every pilot has some piece of data available. (See discussion about
    file_transfer vs pilot-data somewhere else)

    # Parallelism
    'number_of_processes',  # Total number of processes to start
    'processes_per_host',   # Nr of processes per host
    'threads_per_process',  # Nr of threads to start per process
    'total_core_count',     # Total number of cores requested

    AM: Also, shouldn't we just specify a number of cores, and leave actual
    layout to the pilot system?  This would otherwise make automated pilot
    placement very hard...
    MS: No, I would say that we want to offer TROY the possibility of
    launching more than 1 pilot into a resource slice. These saga derived
    notions might not be what we want though, I'm happy to diverge from that.

    # Requirements
    'candidate_hosts',          # List of specific hostnames to run on.
    'cpu_architecture',         # Specify specific arch required.
    'total_physical_memory',

    AM: how is total memory specified?  Is that memory usable for CUs?
    individually / concurrently?
    MS: This is a very good question that I dont have a direct answer on.
    My hunch is that this should be related to the layout of the
    cores/processes/hosts, etc., but that might become messy.
    We need to be able to express memory requirements for the pilot in some
    way though!

    'operating_system_type',  # Specify specific OS required.
    'wall_time_limit',        # Pilot is not needed longer than X.
    'queue'                   # Specify queue name of backend system.

    AM: I think pilot description should be fully reduced to
    a description of the resource slice to be managed by the pilot,
    w/o any details on the actual pilot startup etc.
    MS: I dont think I agree, I feel you are confusing TROY and Sinon again,
    somebody needs to instruct Sinon about resource specifics. Note that we
    already did get rid of some of the members and some more candidates are
    left.

    """


# ------------------------------------------------------------------------------
#
class ComputePilot():
    """This represents instances of ComputePilots.

        MS: capacity?

    """

    def __init__(self, pilot_id=None):
        """Constructor for the ComputePilot.

        Keyword argument::

            id(string): if specified, don't create a new ComputePilot but
            instantiate it based on the id.
            MS: Similar to the discussion of the constructor in the
            CU.

        Return::
            cp(ComputePilot): the ComputePilot object


        """
        pass

    def get_state(self):
        """Return state of PC.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def cancel(self):
        """Cancel the CP.

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
        """Returns an ID (string) for this instance.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    # MS: BigJob has a get_url() to get a "persistent" uri of a CP

    def get_description(self):
        """Returns a ComputePilotDescription for this instance.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def wait(self, timeout=-1.0, state='RUNNING'):
        """Returns when the pilot reaches the specified state, or after timeout
        seconds, whichever comes first.  Calls with timeout<0.0 will wait
        forever.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def submit_unit(self, unit_desc):
        """Submit a CUD and returns a CU.

        Keyword argument(s)::

            unit_desc(ComputeUnitDescription): The CUD.
            or
            unit_desc(DataUnitDescription): The DUD.
            or
            unit_desc([ComputeUnitDescription]): The list of CUDs.
            or
            unit_desc([DataUnitDescription]): The list of DUDs.

        Return::

            unit_id(ID): The ID of the Unit submitted.
            or
            unit_id([ID]): The list of IDs of the Units submitted.



        """
        pass

    def cancel_unit(self, unit_id):
        """Cancel a CU belonging to this CP.


        Keyword argument::

            unit_id(id): ID of CU to cancel.
            or
            unit_id([id]): List of IDs from CUs to cancel.

        Return::

            None

        Raises::
            
            DoesNotExist: No unit with that ID under this pilot. 
            IncorrectState: Unit is already in final state.


        """


# ------------------------------------------------------------------------------
#
class PilotService():
    """PilotService()

        Factory for ComputePilot and DataPilot instances.
    """
    
    def __init__(self):
        """Constructor for the PilotService.
        
        This could take arguments to reconnect to an existing PS.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def submit_pilot(self, pilot_description, context=None):
        """Instantiate and return ComputePilot object.


        Keyword argument(s)::

            pilot_description(ComputePilotDescription): Instantiate a ComputePilot.
            pilot_description([ComputePilotDescription]): Instantiate ComputePilots in bulk.
            pilot_description(DataPilotDescription): Instantiate a DataPilot.
            pilot_description([DataPilotDescription]): Instantiate DataPilots in bulk.
            context(Context): The security context to use for the Pilot(s) on the
                              backend.

        Return::

            pilot_id(ID): An ID representing a Compute or Data Pilot.
            pilot_id([ID]): A list of IDs representing a Compute or Data Pilots
                            that were submitted in bulk.

        """
        pass

    def cancel_pilot(self, pilot_id):
        """Cancel (a) ComputePilot(s).


        Keyword argument(s)::

            pilot_id(ID): The ID of the Pilot to cancel.
            pilot_id([ID]): The IDs of the Pilots to cancel.

        Return::

            None

        """
        pass

    # AM: as discussed, this should not have state, but should have an ID for
    #     reconnect [I see a case for state, TBD]

    def cancel(self):
        """Cancel the PS (self).

        This also cancels the ...

        AM: We should also be able to cancel the PS w/o canceling the
            pilots! [I agree]

        Keyword argument(s)::

            None

        Return::

            None

        """
        pass

    def list_pilots(self):
        """Return a list of ComputePilot IDs managed by this PS.

        Keyword argument::

            None

        Return::

            pilots([Compute Pilot IDs]): List of IDs of CPs.
            or
            None

        """
        pass

    def get_pilot(self, pilot_id):
        """Get (a) Pilot instance(s) based on ID.

        This method is required as based on the ID only we don't know which
        Pilot Service a Pilot belongs to.

        Keyword argument::

            pilot_id(ID): The ID of the Pilot we want to acquire an
                              instance of.
            pilot_id([ID]): The IDs of the Pilot we want to acquire
                                instances of.

        Return::

            pilot(ComputePilot): A ComputePilot object.
            or
            pilots([ComputePilot]): A list of ComputePilot objects.

        """
        pass

    def wait(self, state):
        """Wait for all U's under this PS to reach a certain state.

        ComputePilot: State = 'FINAL': all CUs are done.
        DataPilot: state='RUNNING': all DUs have finished transfers.

        Keyword argument(s)::

            state(STATE): The state to wait for.

        Return::

            name(type): description
            or
            None

        """
        pass


# ------------------------------------------------------------------------------
#
class DataPilotDescription(dict):
    """DataPilotDescription.
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

    # AM: also, what about affinities on CU / DU level, where are those
    # expressed?

    # AM: lifetime, resource information, etc.

    """

    def __init__(self):
        """DPD constructor.

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
    """Object representing a physical storage resource.

    MS: capacity?

    # Class members
    #    'id',           # Reference to this PJ
    #    'description',  # Description of PilotStore
    #    'context',      # SAGA context
    #    'resource_url', # Resource  URL
    #    'state',        # State of the PilotStore

    """

    def __init__(self):
        """DP Constructor.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None
            Add a Data Unit to this Data Pilot.

        """
        pass

    def submit_unit(self, dud):
        """Add a Data Unit to this Data Pilot.

        AM: What does that do exactly?  When are data staged (if at all)?
            What state needs the DU to be in?  Can that fail?

        This brings

        Keyword argument(s)::

            dud(DataUnit Desc): description

        Return::

            None

        """
        pass

    def cancel_unit(self, du_id):
        """Remove a Data Unit from this Data Pilot.

        MS: What should the (optional) semantics of this call be?


        Keyword argument(s)::

            du_id(DataUnit): description

        Return::

            None

        """
        pass

    def list_units(self):
        """List Data Units in this Data Pilot.


        Keyword argument(s)::

            None

        Return::

            units([DU IDs]): List of DUs in this DP.
            or
            None

        """
        pass

    def wait(self):
        """Wait for pending data transfers.

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
        """Cancel DataPilot

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
        """Return the state of the DataPilot.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass

    def split_unit(self, unit_id, num_of_chunks=None, size_of_chunks=None):
        """Split the DU unit in a set of DUs based on the number of chunks
        and chunk size.

        Keyword arguments::

            unit_id(DU id): the DU to split.
            num_of_chunks(int): the number of chunks to create.
            size_of_chunks(int): the size of chunks.

            Only one of the two arguments should be specified.

        Return::

            chunks[DU id]: a list of DU id's that were created.

        """
        pass

    def merge_units(self, input_ids):
        """Merge DU units into one DU.

        Keyword arguments::

            input_ids([DU ids]): the DUs to merge.

        Return::

            output_id(DU id): the merged unit.

        """
        pass

    # MS: BigJob has a get_url() to get a "persistent" uri of a DP

    # AM: should be fully symmetric to PS


# ------------------------------------------------------------------------------
#
class UnitService():
    """Service that brings the ComputePilot's and DataPilot's together.

    AM: and adds some scheduling, and enacts DU/CU dependencies.

    """

    def __init__(self, id=None, scheduler='default'):
        """ UnitService constructor.

        The instantiation of the Unit Service object could possibly be
        done to re-connect to a persistently running Unit Service.
        The constructor would need accommodate that.

        Keyword argument::

            id:        reconnect to an existing UnitService
            scheduler: use the given scheduler.  Only applicabvle if id==None,
                       i.e. if a new UnitService is requested.

        Return::

            None

        Properties:
         
            id:        identifies the service instance for reconnection
            scheduler: the scheduler used in this service instance

        """
        pass

    def cancel(self):
        """Cancel this Unit Service.

        TODO: Would this cancel assigned Units too?

        Keyword argument::

            None

        Return::

            None

        """
        pass

    def add_pilot(self, pilot):
        """Bring a Pilot (and the resources its represents) into the scope of
        the US.

        Note: "pilot" needs to be an instance, because the US would have no
        way to identify the pilot just based on its ID.


        Keyword argument(s)::

            pilot(ComputePilot): A ComputePilot instance.
            or
            pilot(DataPilot): A DataPilot instance.
            or
            pilot([ComputePilot]): A list of ComputePilot instances.
            or
            pilot([DataPilot]): A list of DataPilot instances.

        Return::

            None

        """
        pass

    def remove_pilot(self, pilot_id):
        """Remove a Pilot (and the resources its represents) from the scope of
        the US.

        Keyword argument(s)::

            pilot_id(ID): A CP or DP ID.
            pilot_id([ID]): A list of CP or DP IDs.

        Return::

            None

        """
        pass

    def submit_unit(self, unit_desc):
        """Accepts a CUD or DUD and returns a CU or DU.

        Keyword argument(s)::

            unit_desc(ComputeUnitDescription): The CUD.
            or
            unit_desc(DataUnitDescription): The DUD.
            or
            unit_desc([ComputeUnitDescription]): The list of CUDs.
            or
            unit_desc([DataUnitDescription]): The list of DUDs.

        Return::

            unit_id(ID): The ID of the Unit submitted.
            or
            unit_id([ID]): The list of IDs of the Units submitted.

        """
        pass

    def wait(self):
        """Wait for all the CUs and DUs handled by this UnitService.

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


        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """ 
        pass

    def cancel_unit(self, unit_id):
        """Cancel the specified Compute Unit or Data Unit by its ID.

        Keyword argument(s)::

            unit_id(ID): The Unit to cancel.
            or
            unit_id([ID]): The list of Units to cancel.

         Return::

            None

        """
        pass

    def list_units(self, compute=True, data=True):
        """List the Units handled by this UnitService.


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
        """Get a DU or CU based on its id.

        Keyword argument::

            id(ID): The ID of the unit we want to acquire an instance of.
            or
            id([ID]): The list of IDs of the units we want to acquire instances of.

        Return::

            unit(ComputeUnit): A ComputeUnit object.
            or
            unit(DataUnit): A DataUnit object.
            or
            unit([ComputeUnit]): A list of ComputeUnit objects.
            or
            unit([DataUnit]): A list of DataUnit objects.

        """
        pass

    def list_pilots(self, compute=True, data=True):
        """Return a list Pilot IDs of specified type of Pilots that are
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
        """Get a DP or CP instance based on its ID.

        This method is required as based on the ID only we don't know which
        Pilot Service a Pilot belongs to.

        Keyword argument::

            pilot_id(string): The ID of the Pilot we want to acquire an
                              instance of.
            pilot_id([string]): The list of IDs of the Pilots we want to acquire
                                instances of.

        Return::

            pilot(ComputePilot): A ComputePilot object.
            or
            pilot(DataPilot): A DataPilot object.
            or
            pilot([ComputePilot]): A list of ComputePilot objects.
            or
            pilot([DataPilot]): A list of DataPilot objects.

        """
        pass
