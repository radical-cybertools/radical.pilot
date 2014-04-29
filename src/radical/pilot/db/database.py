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
from pymongo import *
from bson.objectid import ObjectId

from radical.pilot.states import *


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
    def __init__(self, db_url, db_name="radical.pilot"):
        """ Le constructeur. Should not be called directrly, but rather
            via the static methods new() or reconnect().
        """
        self._client = MongoClient(db_url)
        self._db     = self._client[db_name]

        self._session_id = None

        self._s  = None

        self._w  = None
        self._um = None

        self._p  = None
        self._pm = None

    #--------------------------------------------------------------------------
    #
    @staticmethod
    def new(sid, db_url, db_name="radical.pilot"):
        """ Creates a new session (factory method).
        """
        creation_time = datetime.datetime.utcnow()

        dbs = Session(db_url, db_name)
        dbs._create(sid, creation_time)
        return (dbs, creation_time)

    #--------------------------------------------------------------------------
    #
    def _create(self, sid, creation_time):
        """ Creates a new session (private).

            A session is a distinct collection with three sub-collections
            in MongoDB:

            radical.pilot.<sid>    | Base collection. Holds some metadata.   | self._s
            radical.pilot.<sid>.w  | Collection holding all work units.      | self._w
            radical.pilot.<sid>.wm | Collection holding all unit managers.   | self._um
            radical.pilot.<sid>.p  | Collection holding all pilots.          | self._p
            radical.pilot.<sid>.pm | Collection holding all pilot managers.  | self._pm

            All collections are created with a new session. Since MongoDB
            uses lazy-create, they only appear in the database after the
            first insert. That's ok.
        """
        # make sure session doesn't exist already
        if sid in self._db.collection_names():
            raise DBEntryExistsException(
                "Session with id '%s' already exists." % sid)

        # remember session id
        self._session_id = sid

        self._s = self._db["%s" % sid]
        self._s.insert( 
            {
                "_id"  : ObjectId(sid),
                "created"        : creation_time,
                "last_reconnect" : None,
                "credentials"    : []
            }
        )

        # Create the collection shortcut:
        self._w  = self._db["%s.w"  % sid]
        self._um = self._db["%s.wm" % sid] 

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
        return (dbs, session_info)

    #--------------------------------------------------------------------------
    #
    def _reconnect(self, sid):
        """ Reconnects to an existing session (private).
        """
        # make sure session exists
        #if sid not in self._db.collection_names():
        #    raise DBEntryDoesntExistException("Session with id '%s' doesn't exists." % sid)

        self._s = self._db["%s" % sid]
        cursor = self._s.find({"_id": ObjectId(sid)})

        self._s.update({"_id"  : ObjectId(sid)},
                       {"$set" : {"last_reconnect" : datetime.datetime.utcnow()}}
        )

        cursor = self._s.find({"_id": ObjectId(sid)})

        # cursor -> dict
        #if len(cursor) != 1:
        #    raise DBEntryDoesntExistException("Session with id '%s' doesn't exists." % sid)

        self._session_id = sid

        # Create the collection shortcut:
        self._w  = self._db["%s.w"  % sid]
        self._um = self._db["%s.wm" % sid]

        self._p  = self._db["%s.p"  % sid]
        self._pm = self._db["%s.pm" % sid]

        return cursor[0]

    #--------------------------------------------------------------------------
    #
    def session_add_credential(self, credential):
        if self._s is None:
            raise DBException("No active session.")

        self._s.update(
            {"_id": ObjectId(self._session_id)},
            {"$push": {"credentials": credential}},
            multi=True
        )

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
    def insert_pilot_manager(self, pilot_manager_data):
        """ Adds a pilot managers to the list of pilot managers.

            Pilot manager IDs are just kept for book-keeping.
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_manager_json = {"data": pilot_manager_data}
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

        cursor = self._w.find(
            {"_id": ObjectId(unit_uid)},
            {"stdout_id": 1}
        )

        stdout_id = cursor[0]['stdout_id']

        if stdout_id is None:
            return None
        else:
            gfs = gridfs.GridFS(self._db)
            stdout = gfs.get(stdout_id)
            return stdout.read()

    #--------------------------------------------------------------------------
    #
    def get_compute_unit_stderr(self, unit_uid):
        """Returns the ComputeUnit's unit's stderr.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._w.find(
            {"_id": ObjectId(unit_uid)},
            {"stderr_id": 1}
        )

        stderr_id = cursor[0]['stderr_id']

        if stderr_id is None:
            return None
        else:
            gfs = gridfs.GridFS(self._db)
            stderr = gfs.get(stderr_id)
            return stderr.read()

    #--------------------------------------------------------------------------
    #
    def update_pilot_state(self, pilot_uid, started=None, finished=None,
                           submitted=None, state=None, sagajobid=None,
                           sandbox=None, logs=None):

        """Updates the information of a pilot.
        """
        if self._s is None:
            raise Exception("No active session.")

        # construct the update query
        set_query = dict()
        push_query = dict()

        if state is not None:
            set_query["state"] = state
            push_query["statehistory"] = [{'state': state, 'timestamp': datetime.datetime.utcnow()}]

        if started is not None:
            set_query["started"] = started

        if finished is not None:
            set_query["finished"] = finished

        if submitted is not None:
            set_query["submitted"] = submitted

        if sagajobid is not None:
            set_query["sagajobid"] = sagajobid

        if sandbox is not None:
            set_query["sandbox"] = sandbox

        if logs is not None:
            push_query["log"] = logs

        # update pilot entry.
        self._p.update(
            {"_id": ObjectId(pilot_uid)},
            {"$set": set_query, "$pushAll": push_query},
            multi=True
        )

    #--------------------------------------------------------------------------
    #
    def insert_pilot(self, pilot_uid, pilot_manager_uid, pilot_description,
        sandbox):
        """Adds a new pilot document to the database.
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_doc = {
            "_id":            pilot_uid,
            "description":    pilot_description.as_dict(),
            "submitted":      datetime.datetime.utcnow(),
            "input_transfer_started": None,
            "input_transfer_finished": None,
            "started":        None,
            "finished":       None,
            "output_transfer_started": None,
            "output_transfer_finished": None,
            "nodes":          None,
            "cores_per_node": None,
            "sagajobid":      None,
            "sandbox":        sandbox,
            "state":          PENDING,
            "statehistory":   [],
            "log":            [],
            "pilotmanager":   pilot_manager_uid,
            "unitmanager":    None,
            "wu_queue":       [],
            "command":        None,
        }

        self._p.insert(pilot_doc, upsert=False)

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
                pilot_oid.append(ObjectId(pid))
            cursor = self._p.find({"_id": {"$in": pilot_oid}})

        pilots_json = []
        for obj in cursor:
            pilots_json.append(obj)

        return pilots_json

    #--------------------------------------------------------------------------
    #
    def signal_pilots(self, pilot_manager_id, pilot_ids, cmd):
        """ Send a signal to one or more pilots.
        """
        if self._s is None:
            raise Exception("No active session.")

        if pilot_ids is None:
            # send command to all pilots that are known to the
            # pilot manager.
            self._p.update(
                {"pilotmanager": pilot_manager_id},
                {"$set": {"command": cmd}},
                multi=True
            )
        else:
            if not isinstance(pilot_ids, list):
                pilot_ids = [pilot_ids]
            # send command to selected pilots if pilot_ids are
            # specified convert ids to object ids
            for pid in pilot_ids:
                self._p.update(
                    {"_id": ObjectId(pid)},
                    {"$set": {"command": cmd}}
                )

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
                unit_oid.append(ObjectId(wid))

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
    def set_compute_unit_state(self, unit_id, state, log):
        """Update the state and the log of one or more ComputeUnit(s).
        """
        ts = datetime.datetime.utcnow()

        if self._s is None:
            raise Exception("No active session.")

        self._w.update({"_id": ObjectId(unit_id)},
                       {"$set":     {"state": state},
                        "$push": {"statehistory": {"state": state, "timestamp": ts}},
                        "$pushAll": {"log": log}})

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
                unit_oid.append(ObjectId(wid))

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

        cursor = self._um.find({"_id": ObjectId(unit_manager_id)})

        if cursor.count() != 1:
            msg = "No unit manager with id %s found in DB." % unit_manager_id
            raise DBException(msg=msg)

        return cursor[0]

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
            self._p.update({"_id": ObjectId(pilot_id)},
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
            self._p.update({"_id": ObjectId(pilot_id)},
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
    def unit_manager_list_compute_units(self, unit_manager_uid):
        """ Lists all work units associated with a unit manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._w.find({"unitmanager": unit_manager_uid})

        # cursor -> dict
        unit_ids = []
        for obj in cursor:
            unit_ids.append(str(obj['_id']))
        return unit_ids

    #--------------------------------------------------------------------------
    #
    def assign_compute_units_to_pilot(self, pilot_uid, unit_uids):
        """Assigns one or more compute units to a pilot.
        """
        if self._s is None:
            raise Exception("No active session.")

        # Make sure we work on a list.
        if not isinstance(unit_uids, list):
            unit_uids = [unit_uids]

        self._p.update({"_id": ObjectId(pilot_uid)},
                       {"$pushAll":
                           {"wu_queue": [ObjectId(uid) for uid in unit_uids]}})

    #--------------------------------------------------------------------------
    #
    def insert_compute_units(self, pilot_uid, pilot_sandbox, unit_manager_uid,
                             units, unit_log):
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

            working_directory = saga.Url(pilot_sandbox)

            if unit.description.working_directory_priv is not None:
                working_directory.path = unit.description.working_directory_priv
            else:
                working_directory.path += "/unit-"+unit.uid

            unit_json = {
                "_id":          ObjectId(unit.uid),
                "description":  unit.description.as_dict(),
                "unitmanager":  unit_manager_uid,
                "pilot":        pilot_uid,
                "state":        NEW,
                "statehistory": [],
                "submitted":    datetime.datetime.utcnow(),
                "started":      None,
                "finished":     None,
                "exec_locs":    None,
                "exit_code":    None,
                "sandbox":      str(working_directory),
                "stdout_id":    None,
                "stderr_id":    None,
                "log":          unit_log
            }
            unit_docs.append(unit_json)
            results[unit.uid] = unit_json

        unit_uids = self._w.insert(unit_docs)

        assert len(unit_docs) == len(unit_uids)
        assert len(results) == len(unit_uids)

        return results
