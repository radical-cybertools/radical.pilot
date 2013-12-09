#!/usr/bin/env python
# encoding: utf-8

from pymongo import *
from bson.objectid import ObjectId

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
        s = Session(db_url, db_name)
        s._create(sid)
        return s

    #---------------------------------------------------------------------------
    #
    def _create(self, sid):
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
            raise Exception("Session ID '%s' already exists in DB." % sid)

        # remember session id
        self._session_id = sid

        self._s = self._db["%s" % sid]
        self._s.insert({"CREATED": "<DATE>"})

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
        s = Session(db_url, db_name)
        s._reconnect(sid)
        return s

    #---------------------------------------------------------------------------
    #
    def _reconnect(self, sid):
        """ Reconnects to an existing session (private).
        """
        # make sure session exists
        if sid not in self._db.collection_names():
            raise Exception("Session ID '%s' doesn't exists in DB." % sid)

        # remember session id
        self._session_id = sid

        self._s = self._db["%s" % sid]
        self._s.insert({'reconnected': 'DATE'})

        self._w  = self._db["%s.w"  % sid]
        self._um = self._db["%s.wm" % sid]

        self._p  = self._db["%s.p"  % sid]
        self._pm = self._db["%s.pm" % sid] 

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
            raise Exception("No active session.")

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
    def pilot_get_tasks(self, pilot_uid):
        """Get all tasks for the pilot.
        """
        if self._s is None:
            raise Exception("No active session.")

        print "PILOT UID: %s" % pilot_uid

        cursor = self._w.find({"links.pilot": pilot_uid})

        print "RESULTS %s" % cursor

        pilots_json = []
        for obj in cursor:
            pilots_json.append(obj)

        return pilots_json


    #---------------------------------------------------------------------------
    #
    def insert_pilots(self, pilot_manager_uid, pilot_descriptions):
        """ Adds one or more pilots to the database.

            Input is a list of sinon pilot descriptions.

            Inserting any number of pilots costs one roundtrip. 

                (1) Inserting pilot into pilot collection
        """
        if self._s is None:
            raise Exception("No active session.")

        pilot_docs = []
        for pilot_id, pilot_desc in pilot_descriptions.iteritems():
            pilot_json = {
                "_id"           : pilot_id,
                "description"   : pilot_desc['description'].as_dict(),
                "wu_queue"      : [],
                "command"       : None,
                "info"          : {
                    "submitted" : pilot_desc['info']['submitted'],
                    "started"   : None,
                    "finished"  : None,
                    "state"     : pilot_desc['info']['state'],
                    "log"       : pilot_desc['info']['log']
                },
                "links" : {
                    "pilotmanager"  : pilot_manager_uid,
                    "unitmanager"   : None
                }
            }
            pilot_docs.append(pilot_json)
            
        pilot_ids = self._p.insert(pilot_docs)

        # return the object id as a string
        pilot_id_strings = []
        for pilot_id in pilot_ids:
            pilot_id_strings.append(str(pilot_id))
        return pilot_id_strings

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
        # cursor -> dict
        pilots_json = []
        for obj in cursor:
            pilots_json.append(obj)

        return pilots_json

    #---------------------------------------------------------------------------
    #
    def signal_pilots(self, pilot_ids, cmd):
        """ Send a signal to one or more pilots.
        """
        if self._s is None:
            raise Exception("No active session.")

        if not isinstance(pilot_ids, list):
            pilot_ids = [pilot_ids]

        for pilot_id in pilot_ids:
            self._p.update(
                {"_id": ObjectId(pilot_id)}, 
                {"$set": {"command" : cmd}}
            )

    #---------------------------------------------------------------------------
    #
    def get_workunits(self, workunit_manager_uid, workunit_uids=None):
        """ Get yerself a bunch of workunit
        """
        if self._s is None:
            raise Exception("No active session.")

        if workunit_uids == None:
            cursor = self._w.find({"links.unitmanager": workunit_manager_uid})
        else:
            # convert ids to object ids
            workunit_oid = []
            for wid in workunit_uids:
                workunit_oid.append(ObjectId(wid))
            cursor = self._w.find({"_id": {"$in": workunit_oid},
                                   "links.unitmanager": workunit_manager_uid})
        # cursor -> dict
        workunits_json = []
        for obj in cursor:
            workunits_json.append(obj)

        return workunits_json

    #---------------------------------------------------------------------------
    #
    def insert_unit_manager(self, unit_manager_data):
        """ Adds a unit managers to the list of unit managers.

            Unit manager IDs are just kept for book-keeping. 
        """
        if self._s is None:
            raise Exception("No active session.")

        unit_manager_json = {"data" : unit_manager_data}
        result = self._um.insert(unit_manager_json)

        # return the object id as a string
        return str(result)

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
                           {"$set": {"links.unitmanager" : unit_manager_id}})

    #---------------------------------------------------------------------------
    #
    def unit_manager_remove_pilot(self, unit_manager_id, pilot_ids):
        """ Removes a pilot from a unit manager.
        """
        if self._s is None:
            raise Exception("No active session.")

        # Add the ids to the pilot's queue
        for pilot_id in pilot_ids:
            self._p.update({"_id": ObjectId(pilot_id)}, 
                           {"$set": {"links.unitmanager" : None}})

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
    def insert_workunits(self, pilot_id, unit_manager_uid, unit_descriptions):
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

        # Construct and insert workunit documents
        workunit_docs = []
        for key, wu_desc in unit_descriptions.iteritems():
            workunit = {
                "_id"           : key,
                "description"   : {
                    "Executable" : wu_desc['description'].executable,
                    "Arguments"  : wu_desc['description'].arguments, 
                    "Cores"      : wu_desc['description'].cores
                },
                "links"    : {
                    "unitmanager" : unit_manager_uid, 
                    "pilot"       : pilot_id,
                },
                "info"          : {
                    "submitted" : wu_desc['info']['submitted'],
                    "started"   : None,
                    "finished"  : None,
                    "state"     : wu_desc['info']['state'],
                    "log"       : wu_desc['info']['log']
                }
            } 
            workunit_docs.append(workunit)
        wu_ids = self._w.insert(workunit_docs)

        # Add the ids to the pilot's queue
        self._p.update({"_id": ObjectId(pilot_id)}, 
                       {"$pushAll": {"wu_queue" : wu_ids}})
        return wu_ids

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

