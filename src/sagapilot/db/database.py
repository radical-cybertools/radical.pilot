#!/usr/bin/env python
# encoding: utf-8

import datetime
import gridfs
from pymongo import *
from bson.objectid import ObjectId

from sagapilot import states 

# ------------------------------------------------------------------------------
#
def createUID():
    """Returns a new unique id.
    """
    return ObjectId()


# ------------------------------------------------------------------------------
#
class DBException(Exception):

    # --------------------------------------------------------------------------
    #
    def __init__ (self, msg, obj=None) :
        """Le constructeur. Creates a new exception object. 
        """
        Exception.__init__(self, msg)
        self._obj = obj


# ------------------------------------------------------------------------------
#
class DBEntryExistsException(Exception):

    # --------------------------------------------------------------------------
    #
    def __init__ (self, msg, obj=None) :
        """Le constructeur. Creates a new exception object. 
        """
        Exception.__init__(self, msg)
        self._obj = obj

# ------------------------------------------------------------------------------
#
class DBEntryDoesntExistException(Exception):

    # --------------------------------------------------------------------------
    #
    def __init__ (self, msg, obj=None) :
        """Le constructeur. Creates a new exception object. 
        """
        Exception.__init__(self, msg)
        self._obj = obj

#-----------------------------------------------------------------------------
#
class Session():

    #---------------------------------------------------------------------------
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

    #---------------------------------------------------------------------------
    #
    @staticmethod
    def new(sid, db_url, db_name="sinon"):
        """ Creates a new session (factory method).
        """
        creation_time = datetime.datetime.utcnow()

        dbs = Session(db_url, db_name)
        dbs._create(sid, creation_time)
        return (dbs, creation_time)

    #---------------------------------------------------------------------------
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
            raise DBEntryExistsException("Session with id '%s' already exists." % sid)

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

    #---------------------------------------------------------------------------
    #
    @staticmethod
    def reconnect(sid, db_url, db_name="sinon"):
        """ Reconnects to an existing session.

            Here we simply check if a sinon.<sid> collection exists.
        """
        dbs = Session(db_url, db_name)
        session_info = dbs._reconnect(sid)
        return (dbs, session_info)

    #---------------------------------------------------------------------------
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

    #---------------------------------------------------------------------------
    #
    def session_add_credential(self, credential):
        if self._s is None:
            raise DBException("No active session.")

        self._s.update(
            {"_id": ObjectId(self._session_id)}, 
            {"$push": {"credentials": credential}},
            multi=True
        )

    #---------------------------------------------------------------------------
    #
    @property
    def session_id(self):
        """ Returns the session id.
        """
        return self._session_id

    #---------------------------------------------------------------------------
    #
    def delete(self):
        """ Removes a session and all associated collections from the DB.
        """
        if self._s is None:
            raise DBException("No active session.")

        for collection in [self._s, self._w, self._um, self._p, self._pm]:
            collection.drop()
            collection = None

    #---------------------------------------------------------------------------
    #
    def insert_pilot_manager(self, pilot_manager_data):
        """ Adds a pilot managers to the list of pilot managers.

            Pilot manager IDs are just kept for book-keeping. 
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_manager_json = {"data" : pilot_manager_data}
        result = self._pm.insert(pilot_manager_json)

        # return the object id as a string
        return str(result)

    #---------------------------------------------------------------------------
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

    #---------------------------------------------------------------------------
    #
    def pilot_set_state(self, pilot_uid, state):
        """Updates the state of one or more pilots.
        """
        if self._s is None:
            raise Exception("No active session.")

        self._p.update({"_id": ObjectId(pilot_uid)}, 
            {"$set": {"info.state" : state}})

    #---------------------------------------------------------------------------
    #
    def workunit_set_state(self, workunit_uid, state):
        """Updates the state of one or more pilots.
        """
        if self._s is None:
            raise Exception("No active session.")

        self._w.update({"_id": ObjectId(workunit_uid)}, 
            {"$set": {"info.state" : state}})

    #---------------------------------------------------------------------------
    #
    def get_workunit_stdout(self, workunit_uid):
        """Returns the WorkUnit's unit's stdout.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._w.find(
            {"_id": ObjectId(workunit_uid)},
            {"info.stdout_id"}
        )

        stdout_id = cursor[0]['info']['stdout_id']

        if stdout_id is None:
            return None
        else:
            gfs = gridfs.GridFS(self._db)
            stdout = gfs.get(stdout_id)
            return stdout.read()

    #---------------------------------------------------------------------------
    #
    def get_workunit_stderr(self, workunit_uid):
        """Returns the WorkUnit's unit's stderr.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._w.find(
            {"_id": ObjectId(workunit_uid)},
            {"info.stderr_id"}
        )

        stderr_id = cursor[0]['info']['stderr_id']

        if stderr_id is None:
            return None
        else:
            gfs = gridfs.GridFS(self._db)
            stderr = gfs.get(stderr_id)
            return stderr.read()

    #---------------------------------------------------------------------------
    #
    def pilot_get_tasks(self, pilot_uid):
        """Get all tasks for the pilot.
        """
        if self._s is None:
            raise Exception("No active session.")

        cursor = self._w.find({"links.pilot": pilot_uid})

        pilots_json = []
        for obj in cursor:
            pilots_json.append(obj)

        return pilots_json


    def update_pilot_state(self, pilot_uid, started=None, finished=None, submitted=None, state=None, sagajobid=None, logs=None):
        """Updates the information of a pilot.
        """
        if self._s is None:
            raise Exception("No active session.")

        # construct the update query 
        set_query = dict()
        push_query = dict()

        if state is not None:
            set_query["info.state"]     = state

        if started is not None:
            set_query["info.started"]   = started

        if finished is not None:
            set_query["info.finished"]  = finished

        if submitted is not None:
            set_query["info.submitted"] = submitted

        if sagajobid is not None:
            set_query["info.sagajobid"] = sagajobid

        if logs is not None:
            push_query["info.log"]     = logs 

        # update pilot entry.
        self._p.update(
            {"_id": ObjectId(pilot_uid)}, 
            {"$set": set_query, "$pushAll": push_query},
            multi=True
        )

    #---------------------------------------------------------------------------
    #
    def insert_new_pilots(self, pilot_manager_uid, pilot_descriptions):
        """Adds one or more pilots to the database.
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_json_list = []
        
        for pilot_id, pilot_desc in pilot_descriptions.iteritems():
            pilot_json = {
                "_id"           : pilot_id,
                "description"   : pilot_desc['description'],
                "info"          : 
                {
                    "submitted"      : pilot_desc['info']['submitted'],
                    "nodes"          : None,
                    "cores_per_node" : None,
                    "started"        : None,
                    "finished"       : None,
                    "state"          : states.PENDING, #pilot_desc['info']['state'],
                    "log"            : []#pilot_desc['info']['log']
                },
                "links" : 
                {
                    "pilotmanager"   : pilot_manager_uid,
                    "unitmanager"    : None
                },
                "wu_queue"      : [],
                "command"       : None,
            }

            self._p.insert(pilot_json, upsert=False)
            pilot_json_list.append(pilot_json)

        return pilot_json_list

    #---------------------------------------------------------------------------
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

    #---------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_manager_id, pilot_ids=None):
        """ Get a pilot
        """
        if self._s is None:
            raise Exception("No active session.")

        if pilot_ids == None:
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

    #---------------------------------------------------------------------------
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
                {"$set": {"command" : cmd}},
                multi=True
            )
        else:
            if not isinstance(pilot_ids, list):
                pilot_ids = [pilot_ids]
            # send command to selected pilots if pilot_ids are 
            # specified 
            # convert ids to object ids
            for pid in pilot_ids:
                self._p.update(
                    {"_id": ObjectId(pid) },
                    {"$set": {"command" : cmd}}
                )

    #---------------------------------------------------------------------------
    #
    def get_workunits(self, workunit_manager_id, workunit_ids=None):
        """ Get yerself a bunch of workunit
        """
        if self._s is None:
            raise Exception("No active session.")

        if workunit_ids == None:
            cursor = self._w.find(
                {"links.unitmanager": workunit_manager_id}
            )

        else:
            # convert ids to object ids
            workunit_oid = []
            for wid in workunit_ids:
                workunit_oid.append(ObjectId(wid))

            cursor = self._w.find(
                {"_id": {"$in": workunit_oid},
                 "links.unitmanager": workunit_manager_id}
            )

        workunits_json = []
        for obj in cursor:
            workunits_json.append(obj)

        return workunits_json

    #---------------------------------------------------------------------------
    #
    def get_workunit_states(self, workunit_manager_id, workunit_ids=None):
        """ Get yerself a bunch of workunit
        """
        if self._s is None:
            raise Exception("No active session.")

        if workunit_ids == None:
            cursor = self._w.find(
                {"links.unitmanager": workunit_manager_id},
                {"info.state"}
            )

        else:
            # convert ids to object ids
            workunit_oid = []
            for wid in workunit_ids:
                workunit_oid.append(ObjectId(wid))

            cursor = self._w.find(
                {"_id": {"$in": workunit_oid},
                 "links.unitmanager": workunit_manager_id},
                {"info.state"}
            )

        workunit_states = []
        for obj in cursor:
            workunit_states.append(obj['info']['state'])

        return workunit_states

    #---------------------------------------------------------------------------
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

    #---------------------------------------------------------------------------
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

    #---------------------------------------------------------------------------
    #
    def get_pilot_manager(self, pilot_manager_id):
        """ Get a pilot manager.
        """
        if self._s is None:
            raise DBException("No active session.")

        cursor = self._pm.find({"_id": ObjectId(pilot_manager_id)})

        if cursor.count() != 1:
            msg = "No pilot manager with id %s found in DB." % pilot_manager_id
            raise DBException(msg=msg)

        return cursor[0]

    #---------------------------------------------------------------------------
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

    #---------------------------------------------------------------------------
    #
    def unit_manager_add_pilots(self, unit_manager_id, pilot_ids):
        """ Adds a pilot from a unit manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        for pilot_id in pilot_ids:
            self._p.update({"_id": ObjectId(pilot_id)}, 
                           {"$set": {"links.unitmanager" : unit_manager_id}}, True)

    #---------------------------------------------------------------------------
    #
    def unit_manager_remove_pilots(self, unit_manager_id, pilot_ids):
        """ Removes one or more pilots from a unit manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        # Add the ids to the pilot's queue
        for pilot_id in pilot_ids:
            self._p.update({"_id": ObjectId(pilot_id)}, 
                           {"$set": {"links.unitmanager" : None}}, True)

    #---------------------------------------------------------------------------
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

    #---------------------------------------------------------------------------
    #
    def unit_manager_list_work_units(self, unit_manager_uid):
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

    #---------------------------------------------------------------------------
    #
    def get_raw_pilots(self, pilot_ids=None):
        """ Returns the raw pilot documents.

            Great for debugging shit. 
        """
        pilots = []
        if pilot_ids is not None:
            cursor = self._p.find({"_id": { "$in": pilot_ids}})
        else:
            cursor = self._p.find()

        # cursor -> dict
        for obj in cursor:
            pilots.append(obj)
        return pilots

    #---------------------------------------------------------------------------
    #
    def assign_compute_unit_to_pilot(self, compute_unit_uid, pilot_uid):
        """Assigns a compute unit to a pilot.
        """
        # Add the ids to the pilot's queue
        self._p.update({"_id": ObjectId(pilot_uid)}, 
                       {"$push": {"wu_queue" : ObjectId(compute_unit_uid)}})

    #---------------------------------------------------------------------------
    #
    def insert_compute_unit(self, pilot_uid, unit_manager_uid, unit_uid, unit_description, unit_state, unit_log):
        """ Adds one or more workunits to the database.

            A workunit must have the following format:

            {
                "description": sinon.wu_description,  # work_unit description
                "queue_id"   : <queue_id>,            # the assigned queue
            }

            Inserting any number of work units costs 
            1 * (number of different pilots) round-trips: 

                (1) Inserting work units into the work unit collection
                (2) Add work unit id's to the pilot's queue.
        """
        if self._s is None:
            raise Exception("No active session.")

        workunit = {
            "_id"           : ObjectId(unit_uid),
            "description"   : unit_description,
            "links"    : {
                "unitmanager" : unit_manager_uid, 
                "pilot"       : pilot_uid,
            },
            "info"          : {
                "submitted"     : datetime.datetime.utcnow(),
                "started"       : None,
                "finished"      : None,
                "exec_locs"     : None,
                "state"         : unit_state,
                "log"           : unit_log
            }
        } 

        self._w.insert(workunit)
        
    #---------------------------------------------------------------------------
    #
    def get_raw_workunits(self, workunit_ids=None):
        """ Returns the raw workunit documents.

            Great for debugging shit. 
        """
        workunits = []
        if workunit_ids is not None:
            cursor = self._w.find({"_id": { "$in": workunit_ids}})
        else:
            cursor = self._w.find()

        # cursor -> dict
        for obj in cursor:
            workunits.append(obj)
        return workunits

