#pylint: disable=C0301, C0103, W0212

"""
.. module:: sinon.database
   :platform: Unix
   :synopsis: Database functions.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import datetime
import gridfs
from pymongo import *
from bson.objectid import ObjectId

from sagapilot import states


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
    def __init__(self, db_url, db_name="sinon"):
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
    def new(sid, db_url, db_name="sinon"):
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

            sinon.<sid>    | Base collection. Holds some metadata.   | self._s
            sinon.<sid>.w  | Collection holding all work units.      | self._w
            sinon.<sid>.wm | Collection holding all unit managers.   | self._um
            sinon.<sid>.p  | Collection holding all pilots.          | self._p
            sinon.<sid>.pm | Collection holding all pilot managers.  | self._pm

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
                "info" : 
                {
                    "created"        : creation_time,
                    "last_reconnect" : None
                },
                "credentials" : []
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
    def reconnect(sid, db_url, db_name="sinon"):
        """ Reconnects to an existing session.

            Here we simply check if a sinon.<sid> collection exists.
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
                       {"$set" : {"info.last_reconnect" : datetime.datetime.utcnow()}}
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
            {"info.stdout_id"}
        )

        stdout_id = cursor[0]['info']['stdout_id']

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
            {"info.stderr_id"}
        )

        stderr_id = cursor[0]['info']['stderr_id']

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
                           logs=None):

        """Updates the information of a pilot.
        """
        if self._s is None:
            raise Exception("No active session.")

        # construct the update query
        set_query = dict()
        push_query = dict()

        if state is not None:
            set_query["info.state"] = state

        if started is not None:
            set_query["info.started"] = started

        if finished is not None:
            set_query["info.finished"] = finished

        if submitted is not None:
            set_query["info.submitted"] = submitted

        if sagajobid is not None:
            set_query["info.sagajobid"] = sagajobid

        if logs is not None:
            push_query["info.log"] = logs

        # update pilot entry.
        self._p.update(
            {"_id": ObjectId(pilot_uid)},
            {"$set": set_query, "$pushAll": push_query},
            multi=True
        )

    #--------------------------------------------------------------------------
    #
    def insert_pilot(self, pilot_manager_uid, pilot_description):
        """Adds a new pilot document to the database.
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_uid = ObjectId()

        pilot_doc = {
            "_id":            pilot_uid,
            "description":    pilot_description.as_dict(),
            "info":
            {
                "submitted":      datetime.datetime.utcnow(),
                "started":        None,
                "finished":       None,
                "nodes":          None,
                "cores_per_node": None,
                "state":          states.PENDING,
                "log":            []
            },
            "links":
            {
                "pilotmanager":   pilot_manager_uid,
                "unitmanager":    None
            },
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
            cursor = self._p.find({"links.pilotmanager": pilot_manager_uid})
        else:
            cursor = self._p.find()

        # cursor -> dict
        for obj in cursor:
            pilot_ids.append(str(obj['_id']))
        return pilot_ids

    #--------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_manager_id, pilot_ids=None):
        """ Get a pilot
        """
        if self._s is None:
            raise Exception("No active session.")

        if pilot_ids is None:
            cursor = self._p.find({"links.pilotmanager": pilot_manager_id})
        else:
            # convert ids to object ids
            pilot_oid = []
            for pid in pilot_ids:
                pilot_oid.append(ObjectId(pid))
            cursor = self._p.find({"_id": {"$in": pilot_oid},
                                   "links.pilotmanager": pilot_manager_id})

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
                {"links.pilotmanager": pilot_manager_id},
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
                {"links.unitmanager": unit_manager_id}
            )

        else:
            # convert ids to object ids
            unit_oid = []
            for wid in unit_ids:
                unit_oid.append(ObjectId(wid))

            cursor = self._w.find(
                {"_id": {"$in": unit_oid},
                 "links.unitmanager": unit_manager_id}
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
        if self._s is None:
            raise Exception("No active session.")

        self._w.update({"_id": ObjectId(unit_id)},
                       {"$set":     {"info.state": state},
                        "$pushAll": {"info.log": log}})

    #--------------------------------------------------------------------------
    #
    def get_compute_unit_states(self, unit_manager_id, unit_ids=None):
        """ Get yerself a bunch of compute units.
        """
        if self._s is None:
            raise Exception("No active session.")

        if unit_ids is None:
            cursor = self._w.find(
                {"links.unitmanager": unit_manager_id},
                {"info.state"}
            )

        else:
            # convert ids to object ids
            unit_oid = []
            for wid in unit_ids:
                unit_oid.append(ObjectId(wid))

            cursor = self._w.find(
                {"_id": {"$in": unit_oid},
                 "links.unitmanager": unit_manager_id},
                {"info.state"}
            )

        unit_states = []
        for obj in cursor:
            unit_states.append(obj['info']['state'])

        return unit_states

    #--------------------------------------------------------------------------
    #
    def insert_unit_manager(self, unit_manager_data):
        """ Adds a unit managers to the list of unit managers.

            Unit manager IDs are just kept for book-keeping.
        """
        if self._s is None:
            raise Exception("No active session.")

        result = self._um.insert(unit_manager_data)

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
                           {"$set": {"links.unitmanager": unit_manager_id}},
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
                           {"$set": {"links.unitmanager": None}}, True)

    #--------------------------------------------------------------------------
    #
    def unit_manager_list_pilots(self, unit_manager_uid):
        """ Lists all pilots associated with a unit manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._p.find({"links.unitmanager": unit_manager_uid})

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

        cursor = self._w.find({"links.unitmanager": unit_manager_uid})

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
    def insert_compute_units(self, pilot_uid, unit_manager_uid,
                             unit_descriptions, unit_log):
        """ Adds one or more compute units to the database and sets their state
            to 'PENDING'.
        """
        if self._s is None:
            raise Exception("No active session.")

        # Make sure we work on a list.
        if not isinstance(unit_descriptions, list):
            unit_descriptions = [unit_descriptions]

        unit_docs = list()

        for unit_description in unit_descriptions:

            unit = {
                "description": unit_description.as_dict(),
                "links": {
                    "unitmanager": unit_manager_uid,
                    "pilot":       pilot_uid,
                },
                "info": {
                    "state":       states.PENDING,
                    "submitted":   datetime.datetime.utcnow(),
                    "started":     None,
                    "finished":    None,
                    "exec_locs":   None,
                    "exit_code":   None,
                    "stdout_id":   None,
                    "stderr_id":   None,
                    "log":         unit_log
                }
            }
            unit_docs.append(unit)

        unit_uids = self._w.insert(unit_docs)

        return unit_uids
