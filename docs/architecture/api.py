"""

DEPRECATED!!!
GO INTO THE REAL CODE (API/SP1)

Discussion
----------


RADICAL-Pilot API spec
===================


Pilot States
------------
"""

UNKNOWN = 'Unknown'
"""
No state information could be obtained for that pilot.
"""

PENDING = 'Pending'

""" 
This state identifies a newly constructed pilot which is not yet Running, but is
waiting to acquire control over the target resource.  
This state corresponds to the BES state Pending.  This state is initial.
"""

RUNNING = 'Running'
"""
The pilot has successfully acquired control over the target resource,  and can
serve unit requests.  
This state corresponds to the BES state Running.
"""

DONE = 'Done'
"""
The pilot has finished, and does not utilize any resources on the target
resource anymore.  It finished due to 'natural causes' -- for example, it might
have reached the end of its designated lifetime.
This state corresponds to the BES state Finished. This state is final.
"""

CANCELED = 'Canceled'
"""
The pilot has been canceled, i.e. cancel() has been called on the job instance. 
This state corresponds to the BES state Canceled. This state is final.
"""

FAILED = 'Failed' 
"""
The pilot has abnormally finished -- it either met an internal error condition
which caused it to abort, or it has been unexpectedly killed by the resource
manager.
This state corresponds to the BES state Failed. This state is final.
"""


"""
Compute Unit States
-------------------

These states are exactly the same as for the pilot, see above -- the
documentation below clarifies the specific meaning of the states for Compute
Units.

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
"""


"""
Data Unit States
----------------

Data Unit states differ slightly from the Compute Unit states: DUs don't have
a DONE state (as DUs don't have a lifetime), and the CU state 'RUNNING'
corresponds to the DU state 'Avalable'.

The documentation below again documents the exact meaning of the states for DUs.

* Unknown
  No state information could be obtained for that unit.

* Pending
  Data are not yet available, but are scheduled for transfer, or transfer is in
  progress.

* Running
  All DU content is available on at least one DataPilot.

* Canceled    
  The data is scheduled for removal and cannot be used anymore.

* Failed  
  The data could not be transferred, and will not become available in the
  future.
"""

"""
Exceptions
----------

As RADICAL-Pilot is obviously based on SAGA, the exceptions are derived from
SAGA's exception model, and can be extended where we see fit.
"""

import radical.saga.exceptions as rse

class IncorrectURL (rse.IncorrectUrl) :
  """
  The given URL could not be interpreted, for example due to an incorrect
  / unknown schema. 
  """
  pass

class BadParameter (rse.BadParameter) :
  """
  A given parameter is out of bound or ill formatted.
  """
  pass

class DoesNotExist (rse.DoesNotExist) :
  """
  An operation tried to access a non-existing entity.
  """
  pass

class IncorrectState (rse.IncorrectState) :
  """
  The operation is not allowed on the entity in its current state.
  """
  pass

class PermissionDenied (rse.PermissionDenied) :
  """
  The used identity is not permitted to perform the requested operation.
  """
  pass

class AuthorizationFailed (rse.AuthorizationFailed) :
  """
  The backend could not establish a valid identity.
  """
  pass

class AuthenticationFailed (rse.AuthenticationFailed) :
  """
  The backend could not establish a valid identity.
  """
  pass

class Timeout (rse.Timeout) :
  """
  The interaction with the backend times out.
  """
  pass

class NoSuccess (rse.NoSuccess) :
  """
  Some other error occurred.
  """
  pass


"""
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
class UnitDescription(dict):
    """ 
    Base class for ComputeUnitDescription and DataUnitDescription, to cleanly
    specify the signatures for methods which handle both types.
    """
    pass


# ------------------------------------------------------------------------------
# 
class ComputeUnitDescription(UnitDescription):
    """Task description to instantiate a Compute Unit.
    
    The ComputeUnitDescription is a job/task description loosely based on SAGA
    Job Description.

    Class members:

        # Action description
        'name',                 # Non-unique name/label of CU.
        'executable',           # The "action" to execute
        'arguments',            # Arguments to the "action"
        'cleanup',              # cleanup after the CU has finished
        'environment',          # "environment" settings for the "action"
        'start_time',           # When should the CU start
        'working_directory',    # Where to start the CU

        # I/O
        'input',                # stdin
        'error',                # stderr
        'output',               # stdout
        'file_transfer',        # file transfer, duh!
        'input_data',           # DataUnits for input.
        'output_data',          # DataUnits for output.

        # Parallelism
        'spmd_variation',       # Type and startup mechanism.
        'slots',                # Number of job slots for spmd variations that
                                # support it.

        # Host requirements
        'cpu_architecture',     # Specific requirement for binary
        'operating_system_type',# Specific OS version required?
        'total_physical_memory',# May not be physical, but in sync with saga.
        'wall_time_limit',      # CU will not run longer than this.

        # Startup ordering dependencies
        # (Are only considered within scope of bulk submission.)
        'run_after',            # Names of CUs that need to have finished first.
        'start_after',          # Names of CUs that need to have started first.
        'concurrent_with'       # Names of CUs that need to be run concurrently.

    """


# ------------------------------------------------------------------------------
#
class ComputeUnit():
    """ComputeUnit object that allows for direct operations on CUs.

    Class members:

        id              # Unique ID
        description     # The DUD used to instantiate
        state           # The state of the DU



    """

    def __init__(self, cu_id):
        """
        Compute Unit constructor.

        Use the 'get_unit()' call on the US for bulk and async CU construction.

        Keyword argument(s)::
            id(string):  ID referencing the unit to be referenced by the created
                         object instance..

        Return::
            ComputeUnit: class instance representing the referenced CU.

        Raises::
            DoesNotExist
            AuthorizationDenied
            AuthenticationFailed
            NoSuccess

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

            None

        Return::

            name(type): description
            or
            None

        """
        pass

    def get_metric(self, metric):
        """Return the value for the specified metric.


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
class DataUnitDescription(UnitDescription):
    """Describe a DU used for input or output of a CU.

    Class members:

        name         # A non-unique label.
        file_urls    # Dict of logical and physical filesnames, e.g.:
                       # { 'NAME1' : [ 'google://.../name1.txt',
                       #               'srm://grid/name1.txt' ],
                       #   'NAME2' : [ 'file://.../name2.txt' ] }
        lifetime     # Needs to stay available for at least ...
        cleanup      # Can be removed when cancelled
        size         # Estimated size of DU (in bytes)

    """


# ------------------------------------------------------------------------------
#
class DataUnit():
    """DataUnit is a logical handle to a piece of data without explicitly
    refering to its location.


    Class members:

        id                  # Unique ID
        description         # The DUD used to instantiate
        state               # The state of the DU

    """

    def __init__(self, data_unit_description=None):
        """ Data Unit constructor to reconnect to an existing DU.

        Keyword argument(s)::

            name(type): description

        Return::

            name(type): description
            or
            None

        """
        pass
        """
        self.tc = saga.task.Container()
        if not static:
            for lfn in self.ldir:
                tc.add(lfn.replicate('some resource name???', ASYNC))
        """

    def wait(self, state='AVAILABLE'):
        """Wait for Data Unit to become available..

        Keyword arguments::

            state(DU STATE): The state to wait for.

        Return::

            None

        """
        pass
        """
        return self.tc.wait(state)
        """

    def remove(self):
        """Remove all replicas of all contents in Data Unit.

        Keyword argument::

            None

        Return::

            None

        """
        pass
        """
        if not file.name in self.ldir.list () :
            raise DoesNotExist
        lfn = self.ldir.open (file.name)
        for pfn in lfn.list_replicas :
            lfn.remove_location (pfn, PURGE)
        lfn.remove ()
        """

    def export(self, dest_uri):
        """Export the data of this Data Unit to the specified destination
            location.

        Keyword argument::

            dest_uri(string): the location of where to export the data to.

        Return::

            None

        """
        pass
        """
        for lfn in self.ldir.list () :
            lfn.download (dest_uri + '/' + lfn.name)
        """

    def cancel(self):
        """Stops all services from dealing with this DU. Does not remove the data.

        Keyword argument::

            None

        Return::

            None

        Raises::

            None

        """

    def split(self, num_of_chunks=None, size_of_chunks=None):
        """Split the DU unit in a set of DUs based on the number of chunks
        and chunk size.

        The split method is on the DU because it splits the DU into new
        DUs that will reside on the same DP as the original DU.

        Keyword arguments::

            num_of_chunks(int): the number of chunks to create.
            size_of_chunks(int): the size of chunks.

            Only one of the two arguments should be specified.

        Return::

            chunks[DU id]: a list of DU id's that were created.

        """
        pass
        """
        chunks = []
        for i, lfn in enumerate (self.units[du_id].lfns) :
            chunk_id = i%n_chunks
            if not chunk_id in chunks :
                new_dus[chunk_id] = []
            chunks[chunk_id].append (lfn)

        new_dus = []
        for chunk in chunks
            new_dus.append (DataUnit (chunk)

        return new_dus
        """

    def merge(self, input_ids, data_pilot=None, replicate_to_all=False):
        """Merge DU units into one DU.

        The merge method is on the DU (and not DP or US) because a DU is not necessarily
        associated to A DP or US.

        Keyword arguments::

            input_ids([DU ids]): the DU(s) to merge.
            data_pilot(DP id): DP to replicate to.
            or
            data_pilot([DP ids]): DPs to replicate to.
            replicate_to_all(bool): Replicate to all DPs that are among the DUs to merge.

        Return::

            output_id(DU id): the merged unit.

        Raises::

            NoSuccess: Not enough space to replicate all DUs on this DP.

        """
        pass
        """
        combined = []
        for du_id in input_ids :
            du = DataUnit (du_id)
            compined.append (du.files)
        return DataUnit (combined)
        """


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
        'start_time',               # pilot is not needed before X
        'run_time',                 # pilot is not needed after  X
        'cleanup',
        'project',

        # I/O
        # reconsider for SP2
        'working_directory',
        'error',                    # stderr
        'output',                   # stdout
        'file_transfer',            # out/err staging

        # Parallelism 
        # reconsider for SP2
        'slots'                     # total number of cores
        'spmd_variation',           # expected startup mechanism for CUs (def. None)

        # resource requirements
        'candidate_hosts',          # List of specific hostnames to run on.
        'cpu_architecture',         # Specify specific arch required.
        'total_physical_memory',    # mem for CU usage
        'operating_system_type',    # Specify specific OS required.
        'queue'                     # Specify queue name of backend system.
    """


# ------------------------------------------------------------------------------
#
class ComputePilot():
    """Object representing a physical computing resource.

    Class members::

        id              # Unique ID of this CP
        description     # The description used to instantiate
        state           # The state of this CP
        url             # Persistent URL

    """

    def __init__(self, pilot_id=None):
        """Constructor for the ComputePilot.

        Keyword argument::

            id(string): if specified, don't create a new ComputePilot but
            instantiate it based on the id.

        Return::
            cp(ComputePilot): the ComputePilot object


        """
        pass

    def cancel(self):
        """Cancel the CP.

        Keyword argument(s)::

            None

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

        We keep this wait function, because it is input to the decision to cancel or not.

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
class PilotService():
    """PilotService()

        Factory for ComputePilot and DataPilot instances.
    """
    
    def __init__ (self, url=None):
        """Constructor for the PilotService.
        
        This could take arguments to reconnect to an existing PS.

        Keyword argument(s)::

            url(type): reconnection point

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
            or
            pilot_description([ComputePilotDescription]): Instantiate ComputePilots in bulk.
            or
            pilot_description(DataPilotDescription): Instantiate a DataPilot.
            or
            pilot_description([DataPilotDescription]): Instantiate DataPilots in bulk.
            context(Context): The security context to use for the Pilot(s) on the
                              backend.

        Return::

            pilot_id(ID): An ID representing a Compute or Data Pilot.
            or
            pilot_id([ID]): A list of IDs representing a Compute or Data Pilots
                            that were submitted in bulk.

        """
        pass

    def cancel_pilot(self, pilot_id):
        """Cancel (a) ComputePilot(s).


        Keyword argument(s)::

            pilot_id(ID): The ID of the Pilot to cancel.
            or
            pilot_id([ID]): The IDs of the Pilots to cancel.

        Return::

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
            or
            pilot_id([ID]): The IDs of the Pilot we want to acquire
                                instances of.

        Return::

            pilot(ComputePilot): A ComputePilot object.
            or
            pilots([ComputePilot]): A list of ComputePilot objects.

        """
        pass


# ------------------------------------------------------------------------------
#
class DataPilotDescription(dict):
    """Description to instantiate a data pilot on a physical resource.

    Class members::

        resource_url        # The URL of the service endpoint
        size                # Storage size of DP (in bytes)


    # AGREEMENT: AM to move things over from CPD

    """

# ------------------------------------------------------------------------------
#
class DataPilot():
    """Object representing a physical storage resource.

    Class members::

        id              # Reference to this DP
        description     # Description of DP
        context         # SAGA context
        state           # State of the DP
        url             # Persistent URL to this DP

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
        """
        # make sure we have a replica backend, and can start navigating the
        # replica name space
        self.backend = saga.replica.dir ("irods://irods.host.osg/")
        """

    def wait(self, state=RUNNING, timeout=-1.0):
        """
        Wait for pilot to become active

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
        """
        # cancel all DUs
        for du in self.units.keys () :
            du.cancel ()
        """


# ------------------------------------------------------------------------------
#
class UnitService():
    """Service that brings the ComputePilot's and DataPilot's together.
       and adds some scheduling, and enacts DU/CU dependencies.

    """

    def __init__(self, url=None, scheduler='default'):
        """ UnitService constructor.

        The instantiation of the Unit Service object could possibly be
        done to re-connect to a persistently running Unit Service.
        The constructor would need accommodate that.

        Keyword argument::

            url:       reconnect to an existing UnitService
            scheduler: use the given scheduler.  Only applicabvle if id==None,
                       i.e. if a new UnitService is requested.

        Return::

            None

        Properties:
         
            id:        identifies the service instance for reconnection
            scheduler: the scheduler used in this service instance

        """
        pass


    def add_pilot(self, pilots):
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

    def remove_pilot(self, pids):
        """Remove a Pilot (and the resources its represents) from the scope of
        the US.

        Keyword argument(s)::

            pilot_id(ID): A CP or DP ID.
            or
            pilot_id([ID]): A list of CP or DP IDs.

        Return::

            None

        """
        pass


    def list_pilot (self, compute=True, data=True):
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

    def get_pilot (self, pids):
        """Get a DP or CP instance based on its ID.

        This method is required as based on the ID only we don't know which
        Pilot Service a Pilot belongs to.

        Keyword argument::

            pilot_id(string): The ID of the Pilot we want to acquire an
                              instance of.
            or
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

    def submit_unit(self, uds):
        """Accepts a CUD or DUD and returns a CU or DU.

        Keyword argument(s)::

            uds(ComputeUnitDescription): The CUD.
            or
            uds(DataUnitDescription): The DUD.
            or
            uds([ComputeUnitDescription]): The list of CUDs.
            or
            uds([DataUnitDescription]): The list of DUDs.

        Return::

            unit_id(ID): The ID of the Unit submitted.
            or
            unit_id([ID]): The list of IDs of the Units submitted.

        """
        pass


    def wait_unit (self, uids, state=FINAL, timeout=-1.0):
        """
        Wait for all the CUs and DUs handled by this UnitService.
        """ 
        pass


    def cancel_unit (self, uids):
        """Cancel the specified Compute Unit or Data Unit by its ID.

        Keyword argument(s)::

            unit_id(ID): The Unit to cancel.
            or
            unit_id([ID]): The list of Units to cancel.

         Return::

            None

        """
        pass

    def get_unit (self, uids):
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


# ------------------------------------------------------------------------------
# Attribute interface

# ------------------------------------------------------------------------------
#
# define a couple of constants for the attribute API, mostly for registering
# attributes.
#

# type enums
ANY         = 'any'        # any python type can be set
URL         = 'url'        # URL type (string + URL parser checks)
INT         = 'int'        # Integer type
FLOAT       = 'float'      # float type
STRING      = 'string'     # string, duh!
BOOL        = 'bool'       # True or False or Maybe
ENUM        = 'enum'       # value is any one of a list of candidates
TIME        = 'time'       # seconds since epoch, or any py time thing
                           # which can be converted into such
                           # FIXME: conversion not implemented

# mode enums
WRITEABLE   = 'writeable'  # the consumer of the interface can change
                           # the attrib value
READONLY    = 'readonly'   # the consumer of the interface can not
                           # change the attrib value.  The
                           # implementation can still change it.
FINAL       = 'final'      # neither consumer nor implementation can
                           # change the value anymore
ALIAS       = 'alias'      # variable is deprecated, and alias'ed to
                           # a different variable.

# attrib extensions
EXTENDED    = 'extended'   # attribute added as extension
PRIVATE     = 'private'    # attribute added as private

# flavor enums
SCALAR      = 'scalar'     # the attribute value is a single data element
DICT        = 'dict'       # the attribute value is a dict of data elements
VECTOR      = 'vector'     # the attribute value is a list of data elements

# ------------------------------------------------------------------------------
#
# Callback (Abstract) Class
#
class Callback () :
    """
    Callback base class.

    All objects using the Attribute Interface allow to register a callback for
    any changes of its attributes, such as 'state' and 'state_detail'.  Those
    callbacks can be python call'ables, or derivates of this callback base
    class.  Instances which inherit this base class MUST implement (overload)
    the cb() method.

    The callable, or the callback's cb() method is what is invoked whenever the
    SAGA implementation is notified of an change on the monitored object's
    attribute.

    The cb instance receives three parameters upon invocation:


      - obj: the watched object instance
      - key:  the watched attribute (e.g. 'state' or 'state_detail')
      - val:  the new value of the watched attribute

    If the callback returns 'True', it will remain registered after invocation,
    to monitor the attribute for the next subsequent state change.  On returning
    'False' (or nothing), the callback will not be called again.

    To register a callback on a object instance, use::

      class MyCallback (saga.Callback):

          def __init__ (self):
              pass

          def cb (self, obj, key, val) :
              print " %s\\n %s (%s) : %s"  %  self._msg, obj, key, val

      jd  = saga.job.Description ()
      jd.executable = "/bin/date"

      js  = saga.job.Service ("fork://localhost/")
      job = js.create_job(jd)

      cb = MyCallback()
      job.add_callback(saga.STATE, cb)
      job.run()


    See documentation of the :class:`saga.Attribute` interface for further 
    details and examples.
    """

    def __call__ (self, obj, key, val) :
        return self.cb (obj, key, val)

    def cb (self, obj, key, val) :
        """ This is the method that needs to be implemented by the application

            Keyword arguments::

                obj:  the watched object instance
                key:  the watched attribute
                val:  the new value of the watched attribute

            Return::

                keep:   bool, signals to keep (True) or remove (False) the callback
                        after invocation

            Callback invocation MAY (and in general will) happen in a separate
            thread -- so the application need to make sure that the callback
            code is thread-safe.

            The boolean return value is used to signal if the callback should
            continue to listen for events (return True) , or if it rather should
            get unregistered after this invocation (return False).
        """
        pass


# ------------------------------------------------------------------------------
#
class Attributes (_object) :
    """
    For documentation, see the SAGA Attributes interface documentation.
    """


    # --------------------------------------------------------------------------
    #
    # the GFD.90 attribute interface
    #
    # The GFD.90 interface supports CamelCasing, and thus converts all keys to
    # underscore before using them.
    # 
    def set_attribute (self, key, val) :
        """
        set_attribute(key, val)

        This method sets the value of the specified attribute.  If that
        attribute does not exist, DoesNotExist is raised -- unless the attribute
        set is marked 'extensible' or 'private'.  In that case, the attribute is
        created and set on the fly (defaulting to mode=writeable, flavor=Scalar,
        type=ANY, default=None).  A value of 'None' may reset the attribute to
        its default value, if such one exists (see documentation).

        Note that this method is performing a number of checks and conversions,
        to match the value type to the attribute properties (type, mode, flavor).
        Those conversions are not guaranteed to yield the expected result -- for
        example, the conversion from 'scalar' to 'vector' is, for complex types,
        ambiguous at best, and somewhat stupid.  The consumer of the API SHOULD
        ensure correct attribute values.  The conversions are intended to
        support the most trivial and simple use cases (int to string etc).
        Failed conversions will result in an BadParameter exception.

        Attempts to set a 'final' attribute are silently ignored.  Attempts to
        set a 'readonly' attribute will result in an IncorrectState exception
        being raised.

        Note that set_attribute() will trigger callbacks, if a new value
        (different from the old value) is given.  
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return   self._attributes_i_set        (us_key, val)


    # --------------------------------------------------------------------------
    #
    def get_attribute (self, key) :
        """
        get_attribute(key)

        This method returns the value of the specified attribute.  If that
        attribute does not exist, an DoesNotExist is raised.  It is not an
        error to query an existing, but unset attribute though -- that will
        result in 'None' to be returned (or the default value, if available).
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return   self._attributes_i_get        (us_key)


    # --------------------------------------------------------------------------
    #
    def list_attributes (self) :
        """
        list_attributes ()

        List all attributes which have been explicitly set. 
        """

        return self._attributes_i_list ()


    # --------------------------------------------------------------------------
    #
    def attribute_exists (self, key) :
        """
        attribute_exist (key)

        This method will check if the given key is known and was set explicitly.
        The call will also return 'True' if the value for that key is 'None'.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_exists (us_key)


    # --------------------------------------------------------------------------
    #
    # fold the GFD.90 monitoring API into the attributes API
    #
    def add_callback (self, key, cb) :
        """
        add_callback (key, cb)

        For any attribute change, the API will check if any callbacks are
        registered for that attribute.  If so, those callbacks will be called
        in order of registration.  This registration function will return an
        id (cookie) identifying the callback -- that id can be used to
        remove the callback.

        A callback is any callable python construct, and MUST accept three
        arguments::

            - STRING key: the name of the attribute which changed
            - ANY    val: the new value of the attribute
            - ANY    obj: the object on which this attribute interface was called

        The 'obj' can be any python object type, but is guaranteed to expose
        this attribute interface.

        The callback SHOULD return 'True' or 'False' -- on 'True', the callback
        will remain registered, and will thus be called again on the next
        attribute change.  On returning 'False', the callback will be
        unregistered, and will thus not be called again.  Returning nothing is
        interpreted as 'False', other return values lead to undefined behavior.

        Note that callbacks will not be called on 'Final' attributes (they will
        be called once as that attribute enters finality).
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_add_cb (us_key, cb)


    # --------------------------------------------------------------------------
    #
    def remove_callback (self, key, id) :
        """
        remove_callback (key, id)

        This method allows to unregister a previously registered callback, by
        providing its id.  It is not an error to remove a non-existing cb, but
        a valid ID MUST be provided -- otherwise, a BadParameter is raised.

        If no ID is provided (id == None), all callbacks are removed for this
        attribute.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_del_cb (us_key, id)



    # --------------------------------------------------------------------------
    #
    # Python property interface
    #
    # we assume that properties are always used in under_score notation.
    #
    def __getattr__(self, key):
        """ see L{get_attribute} (key) for details. """

    def __setattr__(self, key, val):
        """ see L{set_attribute} (key, val) for details. """

    def __delattr__(self, key):
        """ see L{remove_attribute} (key) for details. """

    def __str__(self):
        """ return a string representation of all set attributes """

    def as_dict(self):
        """ return a dict representation of all set attributes """



