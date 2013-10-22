#!/usr/bin/env python
# encoding: utf-8

from pymongo import MongoClient

#-----------------------------------------------------------------------------
#

class Session():

    def __init__(self, db_url):
        """ Le constructeur. Should not be called directrly, but rather
            via the static methods new() or reconnect().
        """
        self._client = MongoClient(db_url)
        self._db     = self._client.sinon

        self._collection = None
        self._session_id = None

    @staticmethod
    def new(db_url, sid):
        """ Create a new session.
        """
        s = Session(db_url)
        s._create(sid)
        return s

    def _create(self, sid):
        """ Create a new session (private).
        """
        if sid in self._db.collection_names():
            raise Exception("Session ID '%s' already exists in DB." % sid)
        self._collection = self._db["%s" % sid]
        self._collection.insert({'created': True})
        self._wu_collection = self._db["%s.w" % sid]
        self._pilot_collection = self._db["%s.p" % sid]
        self._session_id = sid

    @staticmethod
    def reconnect(db_url, sid):
        """ Reconnect to an existing session.
        """
        s = Session(db_url)
        s._reconnect(sid)
        return s

    def _reconnect(self, sid):
        """ Reconnect to an existing session (private).
        """
        if sid not in self._db.collection_names():
            raise Exception("Session ID '%s' doesn't exists in DB." % sid)
        self._collection = self._db["%s" % sid]
        self._session_id = sid

    @property
    def session_id(self):
        """ Return the session id.
        """
        return self._session_id

    def delete(self):
        """ Remove session and all associated collections from the DB.
        """
        if self._collection is None:
            raise Exception("No active session.")
        self._collection.drop()
        self._wu_collection.drop()
        self._pilot_collection.drop()

    # ------------------------------------------------------------
    # ------------------------------------------------------------
    # Pilots 
    def add_pilots(self, pilot_entries):
        """ Add one or more pilot entries to the session.

            A pilot entry has the following format:

            {
                "pilot_id"   : "unique string",
                "name"       : "descriptive name"
                "description : {

                }
            }
        """
        pass

    def remove_pilots(self, pilot_ids):
        """ Remove one or more pilot entries from the session.
        """
        pass

    def get_pilots(self, pilot_ids=None):
        """ Get one or more pilot entries. If pilot_ids is None, all
            pilots are returned.

            The returned pilot entry dict has the following format:

            {
                "pilot_id"   : "unique string",
                "name"       : "descriptive name"
                "description : {

                },
                "info"       : {
                    "state:       : "STATE",
                    "started"     : "date",
                    "terminated"  : "date",
                    "working_dir" : "local wd"
                } 
            }
        """
        pass

    def get_pilot_infos(self, pilot_ids=None):
        """ Get the 'info' dict for one or more pilot entries. If 
            pilot_ids is None, infos for all pilots are returned.

            'info' is the part of a pilot entry that can change 
            after it has been added to the database. For example, 
            info.state can change from 'running' to 'finished'. 

            The returned pilot info dict has the following format:

            {
                "pilot_id"    : "id of the pilot to modify",
                "state:       : "STATE",
                "started"     : "date",
                "terminated"  : "date",
                "working_dir" : "local wd"
            }

            An 'info' dict can be modified via the modify_pilot_infos method. 
        """
        pass

    def modify_pilot_infos(self, info_entries):
        """ Modify the 'info' dict of one or more pilot entries. 

            An info entry has the following format:

            {
                "pilot_id"    : "id of the pilot to modify",
                "state:       : "STATE",
                "started"     : "date",
                "terminated"  : "date",
                "working_dir" : "local wd"
            }

            If a field is ommited it won't get modified. 
        """
        pass

    def pilot_command_push(self, commands):
        """ Sends a command to a pilot, i.e., pushes a command 
            to a pilot entry's command field. 

            A command has the following format:

            {
                "pilot_id"  : "id of the pilot to control",
                "command:   : "COMMAND"
            }
        """
        pass

    def pilot_wu_queue_push(self, pilot_id, pilot_ids):
        """ Adds one or more work units to a pilot queue.
        """
        pass
        # (1) put pilot_ids into pilot work queue
        # (2) change 'queue' in work_unit document to pilot_id

    def pilot_wu_queue_pop(self, pilot_id, count):
        """ Returns and removes up to 'count' work units from 
            a pilot queue. 
        """
        # (1) remove pilot_ids from pilot work queue
        pass


    # --------------------------------------------------------------------------
    # WorkUnits 
    #
    def work_units_add(self, work_units):
        """ Add one or more work unit entries to the database.

            A work_unit has the following format:

            {
                "work_unit_id"  : "unique work unit ID",
                "description"   : {
                    ...
                },
                "assignment"    : { 
                    "queue" : "queue id",
                    "pilot" : "pilot id"
                }
            }
        """
        # (1) Add work unit to work unit collection
        # (2) Add work unit id to pilot identified by 'pilot_id'
        ids = self._wu_collection.insert(work_units)
        return ids


    def work_units_update(self, work_unit_updates):
        """ Updates the state of one or more work units.

            A work_unit_update dict has the following format:

            {
                "work_unit_id"    : "ID",
                "state"           : "X"  
            }
        """
        # (1) Update the work units in work unit collection
        pass

    def work_units_get(self, work_unit_ids=None): 
        """ Returns one or more work units.

            The returned work units have the following format:

            {
                "work_unit_id"  : "unique work unit ID",
                "description"   : {
                    ...
                },
                "assignment"    : { 
                    "queue" : "queue id",
                    "pilot" : "pilot id"
                }
                "info"          : {
                    "state" : "STATE"
                    ...
                }
            }
        """
        wus = []
        for obj in self._wu_collection.find():
            wus.append(obj)
        return wus

    # ------------------------------------------------------------
    # ------------------------------------------------------------
    # Queues 
    def add_queues(self, queue_entries):
        """ Add one or more queue entries to the database.

            A queue entry has the following format:

            {
                "queue_id"  : "unique string",
                "name"      : "descriptive name",
                "scheduler" : "scheduler name"
                "pilots"    : ["pilot_id 1", "pilot_id 2", "..."]
            }

        """
        pass

    def remove_queue(self, queue_ids):
        """ Remove one or more queue entries from the database.
        """
        pass

    def get_queues(self, queue_ids=None):
        """ Get one or more queue entries. If pilot_ids is None, all
            pilots are returned. 
        """
        pass

    def attach_pilots_to_queue(self, pilot_queue_pairs):
        """ Attach one or more pilots to one or more queues.

            A pilot_queue_pair has the following format:

            {
                "queue_id"  : "queue ID",
                "pilots"    : ["pilot_id 1", "pilot_id 2", "..."]
            }
        """
        pass

    def detach_pilots_from_queue(self, pilot_queue_pairs):
        """ Detach one or more pilots from one or more queues.
        """
        pass



        
