#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.database
   :platform: Unix
   :synopsis: Database functions.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os 
import saga
import datetime
import gridfs
import radical.utils as ru

from pymongo import *

from radical.pilot.states import *
from radical.pilot.utils  import DBConnectionInfo

COMMAND_CANCEL_PILOT        = "Cancel_Pilot"
COMMAND_CANCEL_COMPUTE_UNIT = "Cancel_Compute_Unit"
COMMAND_KEEP_ALIVE          = "Keep_Alive"
COMMAND_FIELD               = "commands"
COMMAND_TYPE                = "type"
COMMAND_ARG                 = "arg"
COMMAND_TIME                = "time"

# -----------------------------------------------------------------------------
#
class DBException(Exception):

    # -------------------------------------------------------------------------
    #
    def __init__(self, msg, obj=None):
        """Le constructeur. Creates a new exception object.
        """
        Exception.__init__(self, msg)
        self._obj = obj


# -----------------------------------------------------------------------------
#
class DBEntryExistsException(Exception):

    # -------------------------------------------------------------------------
    #
    def __init__(self, msg, obj=None):
        """Le constructeur. Creates a new exception object.
        """
        Exception.__init__(self, msg)
        self._obj = obj


#-----------------------------------------------------------------------------
#
class Session():

    #--------------------------------------------------------------------------
    #
    def __init__(self, db_url, db_name="radicalpilot"):
        """ Le constructeur. Should not be called directrly, but rather
            via the static methods new() or reconnect().
        """

        url = ru.Url (db_url)

        if  db_name :
            url.path = db_name

        mongo, db, dbname, pname, cname = ru.mongodb_connect (url)

        self._client = mongo
        self._db     = db
        self._dburl  = str(url)
        self._dbname = dbname
        if url.username and url.password:
            self._dbauth = "%s:%s" % (url.username, url.password)
        else:
            self._dbauth = None

        self._session_id = None

        self._s  = None

        self._w  = None
        self._um = None

        self._p  = None
        self._pm = None

    #--------------------------------------------------------------------------
    #
    @staticmethod
    def new(sid, name, db_url, db_name="radicalpilot"):
        """ Creates a new session (factory method).
        """
        creation_time = datetime.datetime.utcnow()

        dbs = Session(db_url, db_name)
        dbs.create(sid, name, creation_time)

        connection_info = DBConnectionInfo(
            session_id=sid,
            dbname=dbs._dbname,
            dbauth=dbs._dbauth,
            dburl=dbs._dburl
        )

        return (dbs, creation_time, connection_info)

    #--------------------------------------------------------------------------
    #
    def create(self, sid, name, creation_time):
        """ Creates a new session (private).

            A session is a distinct collection with three sub-collections
            in MongoDB:

            radical.pilot.<sid>    | Base collection. Holds some metadata.   | self._s
            radical.pilot.<sid>.cu | Collection holding all compute units.   | self._w
            radical.pilot.<sid>.um | Collection holding all unit managers.   | self._um
            radical.pilot.<sid>.p  | Collection holding all pilots.          | self._p
            radical.pilot.<sid>.pm | Collection holding all pilot managers.  | self._pm

            All collections are created with a new session. Since MongoDB
            uses lazy-create, they only appear in the database after the
            first insert. That's ok.
        """

        # make sure session doesn't exist already
        if  sid :
            if  self._db[sid].count() != 0 :
                raise DBEntryExistsException ("Session '%s' already exists." % sid)

        # remember session id
        self._session_id = sid

        self._s = self._db["%s" % sid]
        self._s.insert({"_id"       : sid,
                        "name"      : name,
                        "created"   : creation_time,
                        "connected" : creation_time})

        # Create the collection shortcut:
        self._w  = self._db["%s.cu" % sid]
        self._um = self._db["%s.um" % sid] 

        self._p  = self._db["%s.p"  % sid]
        self._pm = self._db["%s.pm" % sid] 

    #--------------------------------------------------------------------------
    #
    @staticmethod
    def reconnect(sid, db_url, db_name="radical.pilot"):
        """ Reconnects to an existing session.

            Here we simply check if a radical.pilot.<sid> collection exists.
        """
        dbs = Session(db_url, db_name)
        session_info = dbs._reconnect(sid)

        connection_info = DBConnectionInfo(
            session_id=sid,
            dbname=dbs._dbname,
            dbauth=dbs._dbauth,
            dburl=dbs._dburl
        )

        return (dbs, session_info, connection_info)

    #--------------------------------------------------------------------------
    #
    def _reconnect(self, sid):
        """ Reconnects to an existing session (private).
        """
        # make sure session exists
        #if sid not in self._db.collection_names():
        #    raise DBEntryDoesntExistException("Session with id '%s' doesn't exists." % sid)

        self._s = self._db["%s" % sid]
        cursor = self._s.find({"_id": sid})

        self._s.update({"_id"  : sid},
                       {"$set" : {"connected" : datetime.datetime.utcnow()}}
        )

        cursor = self._s.find({"_id": sid})

        # cursor -> dict
        #if len(cursor) != 1:
        #    raise DBEntryDoesntExistException("Session with id '%s' doesn't exists." % sid)

        self._session_id = sid

        # Create the collection shortcut:
        self._w  = self._db["%s.cu" % sid]
        self._um = self._db["%s.um" % sid]

        self._p  = self._db["%s.p"  % sid]
        self._pm = self._db["%s.pm" % sid]

        try:
            return cursor[0]
        except:
            raise Exception("Couldn't find Session UID '%s' in database." % sid)

    #--------------------------------------------------------------------------
    #
    @property
    def session_id(self):
        """ Returns the session id.
        """
        return self._session_id

    #--------------------------------------------------------------------------
    #
    def delete(self):
        """ Removes a session and all associated collections from the DB.
        """
        if self._s is None:
            raise DBException("No active session.")

        for collection in [self._s, self._w, self._um, self._p, self._pm]:
            collection.drop()
            collection = None

    #--------------------------------------------------------------------------
    #
    def insert_pilot_manager(self, pilot_manager_data, pilot_launcher_workers):
        """ Adds a pilot managers to the list of pilot managers.

            Pilot manager IDs are just kept for book-keeping.
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_manager_json = {"data": pilot_manager_data,
                              "pilot_launcher_workers": pilot_launcher_workers}
        result = self._pm.insert(pilot_manager_json)

        # return the object id as a string
        return str(result)

    #--------------------------------------------------------------------------
    #
    def list_pilot_manager_uids(self):
        """ Lists all pilot managers.
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_manager_uids = []
        cursor = self._pm.find()

        # cursor -> dict
        for obj in cursor:
            pilot_manager_uids.append(str(obj['_id']))
        return pilot_manager_uids

    #--------------------------------------------------------------------------
    #
    def get_compute_unit_stdout(self, unit_uid):
        """Returns the ComputeUnit's unit's stdout.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._w.find({"_id": unit_uid})

        return cursor[0]['stdout']

    #--------------------------------------------------------------------------
    #
    def get_compute_unit_stderr(self, unit_uid):
        """Returns the ComputeUnit's unit's stderr.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._w.find({"_id": unit_uid})

        return cursor[0]['stderr']

    #--------------------------------------------------------------------------
    #
    def update_pilot_state(self, pilot_uid, started=None, finished=None,
                           submitted=None, state=None, sagajobid=None,
                           pilot_sandbox=None, global_sandbox=None, 
                           logs=None):

        """Updates the information of a pilot.
        """
        if self._s is None:
            raise Exception("No active session.")

        # construct the update query
        set_query  = dict()
        push_query = dict()

        if state :
            set_query["state"] = state
            push_query["statehistory"] = [{'state': state, 'timestamp': datetime.datetime.utcnow()}]

        if logs  : 
            push_query["log"] = logs

        if started        : set_query["started"]        = started 
        if finished       : set_query["finished"]       = finished 
        if submitted      : set_query["submitted"]      = submitted 
        if sagajobid      : set_query["sagajobid"]      = sagajobid 
        if pilot_sandbox  : set_query["sandbox"]        = pilot_sandbox 
        if global_sandbox : set_query["global_sandbox"] = global_sandbox 

        # update pilot entry.
        self._p.update(
            {"_id": pilot_uid},
            {"$set": set_query, "$pushAll": push_query},
            multi=True
        )

    #--------------------------------------------------------------------------
    #
    def insert_pilot(self, pilot_uid, pilot_manager_uid, pilot_description,
        pilot_sandbox, global_sandbox):
        """Adds a new pilot document to the database.
        """
        if self._s is None:
            raise Exception("No active session.")

        ts = datetime.datetime.utcnow()

        # the SAGA attribute interface does not expose private attribs in
        # as_dict().  That semantics may change in the future, for now we copy
        # private elems directly.
        pd_dict = dict()
        for k in pilot_description._attributes_i_list (priv=True):
            pd_dict[k] = pilot_description[k]

        pilot_doc = {
            "_id":            pilot_uid,
            "description":    pd_dict,
            "submitted":      datetime.datetime.utcnow(),
            "input_transfer_started": None,
            "input_transfer_finished": None,
            "started":        None,
            "finished":       None,
            "heartbeat":      None,
            "output_transfer_started": None,
            "output_transfer_finished": None,
            "nodes":          None,
            "cores_per_node": None,
            "sagajobid":      None,
            "sandbox":        pilot_sandbox,
            "global_sandbox": global_sandbox,
            "state":          PENDING_LAUNCH,
            "statehistory":   [{"state": PENDING_LAUNCH, "timestamp": ts}],
            "log":            [],
            "pilotmanager":   pilot_manager_uid,
            "unitmanager":    None,
            "commands":       []
        }

        self._p.insert(pilot_doc)

        return str(pilot_uid), pilot_doc

    #--------------------------------------------------------------------------
    #
    def list_pilot_uids(self, pilot_manager_uid=None):
        """ Lists all pilots for a pilot manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_ids = []

        if pilot_manager_uid is not None:
            cursor = self._p.find({"pilotmanager": pilot_manager_uid})
        else:
            cursor = self._p.find()

        # cursor -> dict
        for obj in cursor:
            pilot_ids.append(str(obj['_id']))
        return pilot_ids

    #--------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_manager_id=None, pilot_ids=None):
        """ Get a pilot
        """
        if self._s is None:
            raise Exception("No active session.")

        if pilot_manager_id is None and pilot_ids is None:
            raise Exception(
                "pilot_manager_id and pilot_ids can't both be None.")

        if pilot_ids is None:
            cursor = self._p.find({"pilotmanager": pilot_manager_id})
        else:

            if not isinstance(pilot_ids, list):
                pilot_ids = [pilot_ids]

            # convert ids to object ids
            pilot_oid = []
            for pid in pilot_ids:
                pilot_oid.append(pid)
            cursor = self._p.find({"_id": {"$in": pilot_oid}})

        pilots_json = []
        for obj in cursor:
            pilots_json.append(obj)

        return pilots_json

    #--------------------------------------------------------------------------
    #
    def send_command_to_pilot(self, cmd, arg=None, pilot_manager_id=None, pilot_ids=None):
        """ Send a command to one or more pilots.
        """
        if self._s is None:
            raise Exception("No active session.")

        if pilot_manager_id is None and pilot_ids is None:
            raise Exception("Either Pilot Manager or Pilot needs to be specified.")

        if pilot_manager_id is not None and pilot_ids is not None:
            raise Exception("Pilot Manager and Pilot can not be both specified.")

        command = {COMMAND_FIELD: {COMMAND_TYPE: cmd,
                                   COMMAND_ARG:  arg,
                                   COMMAND_TIME: datetime.datetime.utcnow()
        }}

        if pilot_ids is None:
            # send command to all pilots that are known to the
            # pilot manager.
            self._p.update(
                {"pilotmanager": pilot_manager_id},
                {"$push": command},
                multi=True
            )
        else:
            if not isinstance(pilot_ids, list):
                pilot_ids = [pilot_ids]
            # send command to selected pilots if pilot_ids are
            # specified convert ids to object ids
            for pid in pilot_ids:
                self._p.update(
                    {"_id": pid},
                    {"$push": command}
                )


    #--------------------------------------------------------------------------
    #
    def publish_compute_pilot_callback_history(self, pilot_uid, callback_history):

        if self._s is None:
            raise Exception("No active session.")

        self._p.update({"_id": pilot_uid},
                       {"$set": {"callbackhistory": callback_history}})

    #--------------------------------------------------------------------------
    #
    def get_compute_units(self, unit_manager_id, unit_ids=None):
        """ Get yerself a bunch of compute units.
        """
        if self._s is None:
            raise Exception("No active session.")

        if unit_ids is None:
            cursor = self._w.find(
                {"unitmanager": unit_manager_id}
            )

        else:
            # convert ids to object ids
            unit_oid = []
            for wid in unit_ids:
                unit_oid.append(wid)

            cursor = self._w.find(
                {"_id": {"$in": unit_oid},
                 "unitmanager": unit_manager_id}
            )

        units_json = []
        for obj in cursor:
            units_json.append(obj)

        return units_json

    #--------------------------------------------------------------------------
    #
    def change_compute_units (self, filter_dict, set_dict, push_dict):
        """Update the state and the log of all compute units belonging to
           a specific pilot.
        """
        ts = datetime.datetime.utcnow()

        if self._s is None:
            raise Exception("No active session.")

        self._w.update(spec     = filter_dict, 
                       document = {"$set" : set_dict, 
                                   "$push": push_dict}, 
                       multi    = True)


    #--------------------------------------------------------------------------
    #
    def set_compute_unit_state(self, unit_ids, state, log, src_states=None):
        """
        Update the state and the log of one or more ComputeUnit(s).
        If src_states is given, this will only update units which are currently
        in those src states.
        """
        ts = datetime.datetime.utcnow()

        if  not unit_ids :
            return

        if  self._s is None:
            raise Exception("No active session.")

        # Make sure we work on a list.
        if not isinstance(unit_ids, list):
            unit_ids = [unit_ids]

        if src_states and not isinstance (src_states, list) :
            src_states = [src_states]

        bulk = self._w.initialize_ordered_bulk_op ()

        for uid in unit_ids :

            if src_states :
                bulk.find   ({"_id"     : uid, 
                              "state"   : {"$in"  : src_states} }) \
                    .update ({"$set"    : {"state": state},
                              "$push"   : {"statehistory": {"state": state, "timestamp": ts}},
                              "$push"   : {"log"  : {"message": log, "timestamp": ts}}})
            else :
                bulk.find   ({"_id"     : uid}) \
                    .update ({"$set"    : {"state": state},
                              "$push"   : {"statehistory": {"state": state, "timestamp": ts}},
                              "$push"   : {"log"  : {"message": log, "timestamp": ts}}})

        result = bulk.execute()

        # TODO: log result.
        # WHY DON'T WE HAVE A LOGGER HERE?


    #--------------------------------------------------------------------------
    #
    def get_compute_unit_states(self, unit_manager_id, unit_ids=None):
        """ Get yerself a bunch of compute units.
        """
        if self._s is None:
            raise Exception("No active session.")

        if unit_ids is None:
            cursor = self._w.find(
                {"unitmanager": unit_manager_id},
                {"state": 1}
            )

        else:
            # convert ids to object ids
            unit_oid = []
            for wid in unit_ids:
                unit_oid.append(wid)

            cursor = self._w.find(
                {"_id": {"$in": unit_oid},
                 "unitmanager": unit_manager_id},
                {"state": 1}
            )

        unit_states = []
        for obj in cursor:
            unit_states.append(obj['state'])

        return unit_states

    #--------------------------------------------------------------------------
    #
    def insert_unit_manager(self, scheduler, input_transfer_workers, output_transfer_workers):
        """ Adds a unit managers to the list of unit managers.

            Unit manager IDs are just kept for book-keeping.
        """
        if self._s is None:
            raise Exception("No active session.")

        result = self._um.insert(
            {"scheduler": scheduler,
             "input_transfer_workers": input_transfer_workers,
             "output_transfer_workers": output_transfer_workers }
        )

        # return the object id as a string
        return str(result)

    #--------------------------------------------------------------------------
    #
    def get_unit_manager(self, unit_manager_id):
        """ Get a unit manager.
        """
        if self._s is None:
            raise DBException("No active session.")

        cursor = self._um.find({"_id": unit_manager_id})

        if cursor.count() != 1:
            msg = "No unit manager with id %s found in DB." % unit_manager_id
            raise DBException(msg=msg)

        try:
            return cursor[0]
        except:
            msg = "No UnitManager with id '%s' found in database." % unit_manager_id
            raise DBException(msg=msg)

    #--------------------------------------------------------------------------
    #
    def get_pilot_manager(self, pilot_manager_id):
        """ Get a unit manager.
        """
        if self._s is None:
            raise DBException("No active session.")

        cursor = self._pm.find({"_id": pilot_manager_id})

        try:
            return cursor[0]
        except:
            msg = "No pilot manager with id '%s' found in DB." % pilot_manager_id
            raise DBException(msg=msg)


    #--------------------------------------------------------------------------
    #
    def list_unit_manager_uids(self):
        """ Lists all pilot managers.
        """
        if self._s is None:
            raise Exception("No active session.")

        unit_manager_uids = []
        cursor = self._um.find()

        # cursor -> dict
        for obj in cursor:
            unit_manager_uids.append(str(obj['_id']))
        return unit_manager_uids

    #--------------------------------------------------------------------------
    #
    def unit_manager_add_pilots(self, unit_manager_id, pilot_ids):
        """ Adds a pilot from a unit manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        for pilot_id in pilot_ids:
            self._p.update({"_id": pilot_id},
                           {"$set": {"unitmanager": unit_manager_id}},
                           True)

    #--------------------------------------------------------------------------
    #
    def unit_manager_remove_pilots(self, unit_manager_id, pilot_ids):
        """ Removes one or more pilots from a unit manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        # Add the ids to the pilot's queue
        for pilot_id in pilot_ids:
            self._p.update({"_id": pilot_id},
                           {"$set": {"unitmanager": None}}, True)

    #--------------------------------------------------------------------------
    #
    def unit_manager_list_pilots(self, unit_manager_uid):
        """ Lists all pilots associated with a unit manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._p.find({"unitmanager": unit_manager_uid})

        # cursor -> dict
        pilot_ids = []
        for obj in cursor:
            pilot_ids.append(str(obj['_id']))
        return pilot_ids

    #--------------------------------------------------------------------------
    #
    def unit_manager_list_compute_units(self, unit_manager_uid, pilot_uid=None):
        """ Lists all compute units associated with a unit manager.
        """
        # FIXME: why is this call not updating local unit state?
        if  self._s is None:
            raise Exception("No active session.")

        if  pilot_uid :
            cursor = self._w.find({"unitmanager": unit_manager_uid, 
                                   "pilot"      : pilot_uid})
        else :
            cursor = self._w.find({"unitmanager": unit_manager_uid})

        # cursor -> dict
        unit_ids = []
        for obj in cursor:
            unit_ids.append(str(obj['_id']))
        return unit_ids

    #--------------------------------------------------------------------------
    #
    def pilot_list_compute_units(self, pilot_uid):
        """ Lists all compute units associated with a unit manager.
        """
        # FIXME: why is this call not updating local unit state?
        if  self._s is None:
            raise Exception("No active session.")

        cursor = self._w.find({"pilot"      : pilot_uid})

        # cursor -> dict
        unit_ids = []
        for obj in cursor:
            unit_ids.append(str(obj['_id']))
        return unit_ids

    #--------------------------------------------------------------------------
    #
    def assign_compute_units_to_pilot(self, units, pilot_uid, pilot_sandbox):
        """Assigns one or more compute units to a pilot.
        """

        if  not units :
            return

        if  self._s is None:
            raise Exception("No active session.")

        # Make sure we work on a list.
        if not isinstance(units, list):
            units = [units]

        bulk = self._w.initialize_ordered_bulk_op ()

        for unit in units :

            bulk.find   ({"_id" : unit.uid}) \
                .update ({"$set": {"description"   : unit.description.as_dict(),
                                   "pilot"         : pilot_uid,
                                   "pilot_sandbox" : pilot_sandbox,
                                   "sandbox"       : unit.sandbox,
                                   "FTW_Input_Status": unit.FTW_Input_Status,
                                   "FTW_Input_Directives": unit.FTW_Input_Directives,
                                   "Agent_Input_Status": unit.Agent_Input_Status,
                                   "Agent_Input_Directives": unit.Agent_Input_Directives,
                                   "FTW_Output_Status": unit.FTW_Output_Status,
                                   "FTW_Output_Directives": unit.FTW_Output_Directives,
                                   "Agent_Output_Status": unit.Agent_Output_Status,
                                   "Agent_Output_Directives": unit.Agent_Output_Directives
                        }})
        result = bulk.execute()

        # TODO: log result.
        # WHY DON'T WE HAVE A LOGGER HERE?


    #--------------------------------------------------------------------------
    #
    def publish_compute_unit_callback_history(self, unit_uid, callback_history):

        if self._s is None:
            raise Exception("No active session.")

        self._w.update({"_id": unit_uid},
                       {"$set": {"callbackhistory": callback_history}})

    #--------------------------------------------------------------------------
    #
    def insert_compute_units(self, unit_manager_uid, units, unit_log):
        """ Adds one or more compute units to the database and sets their state
            to 'PENDING'.
        """
        if self._s is None:
            raise Exception("No active session.")

        # Make sure we work on a list.
        if not isinstance(units, list):
            units = [units]

        unit_docs = list()
        results = dict()

        for unit in units:

            ts = datetime.datetime.utcnow()

            unit_json = {
                "_id":           unit.uid,
                "description":   unit.description.as_dict(),
                "restartable":   unit.description.restartable,
                "unitmanager":   unit_manager_uid,
                "pilot":         None,
                "pilot_sandbox": None,
                "state":         unit._local_state,
                "statehistory":  [{"state": unit._local_state, "timestamp": ts}],
                "submitted":     datetime.datetime.utcnow(),
                "started":       None,
                "finished":      None,
                "exec_locs":     None,
                "exit_code":     None,
                "sandbox":       None,
                "stdout":        None,
                "stderr":        None,
                "log":           unit_log,
                "FTW_Input_Status":        None,
                "FTW_Input_Directives":    None,
                "Agent_Input_Status":      None,
                "Agent_Input_Directives":  None,
                "FTW_Output_Status":       None,
                "FTW_Output_Directives":   None,
                "Agent_Output_Status":     None,
                "Agent_Output_Directives": None
            }
            unit_docs.append(unit_json)
            results[unit.uid] = unit_json

        unit_uids = self._w.insert(unit_docs)

        assert len(unit_docs) == len(unit_uids)
        assert len(results) == len(unit_uids)

        return results
