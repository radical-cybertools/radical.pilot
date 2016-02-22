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
import gridfs
import pymongo
import radical.utils as ru

from radical.pilot.utils  import timestamp
from radical.pilot.states import *

COMMAND_CANCEL_PILOT        = "Cancel_Pilot"
COMMAND_CANCEL_COMPUTE_UNIT = "Cancel_Compute_Unit"
COMMAND_KEEP_ALIVE          = "Keep_Alive"
COMMAND_FIELD               = "commands"
COMMAND_TYPE                = "type"
COMMAND_ARG                 = "arg"
COMMAND_TIME                = "time"


#-----------------------------------------------------------------------------
#
class DBSession(object):

    #--------------------------------------------------------------------------
    #
    def __init__(self, sid, dburl):
        """ Creates a new session

            A session is a MongoDB collection which contains documents of
            different types:

            session : document describing this rp.Session (singleton)
            pmgr    : document describing a rp.PilotManager 
            pilots  : document describing a rp.Pilot
            umgr    : document describing a rp.UnitManager
            units   : document describing a rp.Unit
        """

        # mpongodb_connect wants a string at the moment
        mongo, db, _, _, _ = ru.mongodb_connect(str(dburl))

        if not mongo or not db:
            raise RuntimeError("Could not connect to database at %s" % dburl)

        self._client     = mongo
        self._db         = db
        self._dburl      = ru.Url(dburl)
        self._session_id = sid
        self._created    = timestamp()
        self._connected  = timestamp()
        self._closed     = None

        self._c = self._db[sid] # creates collection (lazily)

        # If session exists, we assume this is a reconnect, otherwise we create
        # the session entry.
        # NOTE: hell will break loose if session IDs are not unique!
        if not self._c.count():

            # make 'uid', 'type' and 'state' indexes, as we frequently query
            # based on combinations of those.  Only 'uid' is unique
            self._c.create_index([('uid',   pymongo.ASCENDING)], unique=True,  sparse=False)
            self._c.create_index([('type',  None             )], unique=False, sparse=False)
            self._c.create_index([('state', None             )], unique=False, sparse=False)

            # insert the session doc
            self._c.insert({"uid"       : sid,
                            "type"      : 'session',
                            "created"   : self._created,
                            "connected" : self._connected})
        else:
            pass
            # FIXME: get self._created from DB
            # FIXME: get bridge addresses from DB (not here though)


    #--------------------------------------------------------------------------
    #
    @property
    def session_id(self):
        """ Returns the session id.
        """
        return self._session_id


    #--------------------------------------------------------------------------
    #
    @property
    def dburl(self):
        """ Returns the session db url.
        """
        return self._dburl


    #--------------------------------------------------------------------------
    #
    def get_db(self):
        """ Returns the session db.
        """
        return self._db


    #--------------------------------------------------------------------------
    #
    @property
    def created(self):
        """ Returns the creation time
        """
        return self._created


    #--------------------------------------------------------------------------
    #
    @property
    def connected(self):
        """ Returns the connection time
        """
        return self._connected


    #--------------------------------------------------------------------------
    #
    @property
    def closed(self):
        """ Returns the connection time
        """
        return self._closed


    #--------------------------------------------------------------------------
    #
    def close(self):
        """ 
        close the session
        """
        if not self._c:
            raise RuntimeError("No active session.")

        self._closed = timestamp()
        self._c = None


    #--------------------------------------------------------------------------
    #
    def delete(self):
        """ 
        Removes a session and all associated collections from the DB.
        """
        if not self._c:
            raise RuntimeError("No active session.")

        self._closed = timestamp()
        self._c.drop()
        self._c = None


    #--------------------------------------------------------------------------
    #
    def insert_pilot_manager(self, pmgr_uid, pmgr_data):
        """ 
        Adds a pilot managers to the list of pilot managers.
        """
        if not self._c:
            raise Exception("No active session.")

        # FIXME: also stor cfg
        result = self._c.insert({'type' : 'pmgr', 
                                 "uid"  : pmgr_uid,
                                 "data" : pmgr_data})

        return result


    #--------------------------------------------------------------------------
    #
    def list_pilot_manager_uids(self):
        """ Lists all pilot managers.
        """
        if not self._c:
            raise Exception("No active session.")

        pilot_manager_uids = []
        cursor = self._c.find({'type' : 'pmgr'})

        return [doc['uid'] for doc in cursor]


    #--------------------------------------------------------------------------
    #
    def get_compute_unit_stdout(self, uid):
        """
        Returns the ComputeUnit's unit's stdout.
        """
        # FIXME: cache
        if not self._c:
            raise Exception("No active session.")

        cursor = self._c.find({'type' : 'unit', 
                               'uid'  : uid})

        return cursor[0]['stdout']


    #--------------------------------------------------------------------------
    #
    def get_compute_unit_stderr(self, uid):
        """
        Returns the ComputeUnit's unit's stderr.
        """
        # FIXME: cache
        if not self._c:
            raise Exception("No active session.")

        cursor = self._c.find({'type' : 'unit', 
                               'uid'  : uid})

        return cursor[0]['stderr']


    #--------------------------------------------------------------------------
    #
    def update_pilot_state(self, pilot_uid, started=None, finished=None,
                           submitted=None,  state=None,   sagajobid=None,
                           pilot_sandbox=None, global_sandbox=None, 
                           logs=None):

        """
        Updates the information of a pilot.
        """
        # FIXME: push the doc?

        if not self._c:
            raise Exception("No active session.")

        # construct the update query
        set_query  = dict()
        push_query = dict()

        if state:
            set_query["state"] = state
            push_query["statehistory"] = [{'state': state, 'timestamp': timestamp()}]

        if logs: 
            push_query["log"] = logs

        if started        : set_query["started"]        = started 
        if finished       : set_query["finished"]       = finished 
        if submitted      : set_query["submitted"]      = submitted 
        if sagajobid      : set_query["sagajobid"]      = sagajobid 
        if pilot_sandbox  : set_query["sandbox"]        = pilot_sandbox 
        if global_sandbox : set_query["global_sandbox"] = global_sandbox 

        # update pilot entry.
        self._c.update({'type'     : 'pilot', 
                        "uid"      : pilot_uid},
                       {"$set"     : set_query, 
                        "$pushAll" : push_query},
                       multi=True)


    #--------------------------------------------------------------------------
    #
    def insert_pilot(self, pilot_uid, pmgr_uid, pilot_description,
                     pilot_sandbox, global_sandbox):
        """
        Adds a new pilot document to the database.
        """

        if not self._c:
            raise Exception("No active session.")

        ts = timestamp()

        # the SAGA attribute interface does not expose private attribs in
        # as_dict().  That semantics may change in the future, for now we copy
        # private elems directly.
        # FIXME: check if fixed
        #
        pd_dict = dict()
        for k in pilot_description._attributes_i_list(priv=True):
            pd_dict[k] = pilot_description[k]

        pilot_doc = {
            "uid":            pilot_uid,
            "type":           'pilot',
            "description":    pd_dict,
            "submitted":      ts,
            "input_transfer_started": None,   # FIXME
            "input_transfer_finished": None,  # FIXME
            "started":        None,
            "finished":       None,
            "heartbeat":      None,
            "output_transfer_started": None,   # FIXME
            "output_transfer_finished": None,  # FIXME
            "nodes":          None,
            "cores_per_node": None,
            "sagajobid":      None,
            "sandbox":        pilot_sandbox,
            "global_sandbox": global_sandbox,
            "state":          PENDING_LAUNCH,
            "statehistory":   [{"state": PENDING_LAUNCH, "timestamp": ts}],
            "log":            [],
            "pmgr":           pmgr_uid,
            "umgr":           None,
            "commands":       []
        }

        self._c.insert(pilot_doc)

        return [pilot_uid, pilot_doc]


    #--------------------------------------------------------------------------
    #
    def list_pilot_uids(self, pmgr_uid=None):
        """ 
        Lists all pilots for a pilot manager.

        Return a list of UIDs
        """

        if not self._c:
            raise Exception("No active session.")

        pilot_ids = []

        if pmgr_uid:
            cursor = self._c.find({'type' : 'pilot', 
                                   "pmgr" : pmgr_uid})
        else:
            cursor = self._c.find({'type' : 'pilot'})

        return [doc['uid'] for doc in cursor]


    #--------------------------------------------------------------------------
    #
    def get_pilots(self, pmgr_uid=None, pilot_ids=None):
        """ Get a pilot
        """
        if not self._c:
            raise Exception("No active session.")

        if not pmgr_uid and not pilot_ids:
            raise Exception(
                "pmgr_uid and pilot_ids can't both be None.")

        if not pilot_ids:
            cursor = self._c.find({'type' : 'pilot', 
                                   "pmgr" : pmgr_uid})
        else:

            if not isinstance(pilot_ids, list):
                pilot_ids = [pilot_ids]

            cursor = self._c.find({'type' : 'pilot', 
                                   "uid"  : {"$in": pilot_ids}})

        # make sure we return every unit doc only once
        # https://www.quora.com/How-did-mongodb-return-duplicated-but-different-documents
        ret = { doc['uid'] : doc for doc in cursor}

        return ret.values


    #--------------------------------------------------------------------------
    #
    def send_command_to_pilot(self, cmd, arg=None, pmgr_uid=None, pilot_ids=None):
        """ 
        Send a command to one or more pilots.
        """
        
        if not self._c:
            raise Exception("No active session.")

        if not pmgr_uid and not pilot_ids:
            raise Exception("Either Pilot Manager or Pilot needs to be specified.")

        if pmgr_uid and pilot_ids:
            raise Exception("Pilot Manager and Pilot can not be both specified.")

        command = {COMMAND_FIELD: {COMMAND_TYPE: cmd,
                                   COMMAND_ARG:  arg,
                                   COMMAND_TIME: timestamp()}}

        if not pilot_ids:
            # send command to all pilots that are known to the
            # pilot manager.
            self._c.update({'type'  : 'pilot', 
                            "pmgr"  : pmgr_uid},
                           {"$push" : command},
                            multi=True)
        else:

            if not isinstance(pilot_ids, list):
                pilot_ids = [pilot_ids]

            # send command to selected pilots if pilot_ids are
            for pid in pilot_ids:
                self._c.update({'type' : 'pilot', 
                                "uid"  : pid},
                               {"$push": command})


    #--------------------------------------------------------------------------
    #
    def publish_compute_pilot_callback_history(self, pilot_uid, callback_history):

        # FIXME

        if not self._c:
            raise Exception("No active session.")

        self._c.update({'type' : 'pilot', 
                        "uid"  : pilot_uid},
                       {"$set" : {"callbackhistory": callback_history}})


    #--------------------------------------------------------------------------
    #
    def get_compute_units(self, umgr_uid, unit_ids=None):
        """
        Get yerself a bunch of compute units.

        return dict {uid:unit}
        """
        if not self._c:
            raise Exception("No active session.")

        if not unit_ids:
            cursor = self._c.find({'type' : 'unit', 
                                   'umgr' : umgr_uid})

        else:
            cursor = self._c.find({'type' : 'unit', 
                                   'uid'  : {"$in": unit_ids},
                                   'umgr' : umgr_uid})

        # make sure we return every unit doc only once
        # https://www.quora.com/How-did-mongodb-return-duplicated-but-different-documents
        ret = { doc['uid'] : doc for doc in cursor}

        return ret.values


    #--------------------------------------------------------------------------
    #
    def change_compute_units(self, filter_dict, set_dict, push_dict):
        """
        Update the state and the log of all compute units belonging to
        a specific pilot.
        """

        if not self._c:
            raise Exception("No active session.")

        # make sure we only operate on units
        filter_dict['type'] = 'unit'

        self._c.update(spec     = filter_dict, 
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
        ts = timestamp()

        if not unit_ids:
            return

        if not self._c:
            raise Exception("No active session.")

        # Make sure we work on a list.
        if not isinstance(unit_ids, list):
            unit_ids = [unit_ids]

        if src_states and not isinstance(src_states, list):
            src_states = [src_states]

        bulk = self._c.initialize_ordered_bulk_op()

        for uid in unit_ids:

            if src_states:
                bulk.find  ({"type"  : 'unit', 
                             "uid"   : uid, 
                             "state" : {"$in"  : src_states} }) \
                    .update({"$set"  : {"state": state},
                             "$push" : {"statehistory": {"state":   state, "timestamp": ts}},
                             "$push" : {"log"         : {"message": log,   "timestamp": ts}}})
            else:
                bulk.find  ({"type"  : 'unit', 
                             "uid"   : uid}) \
                    .update({"$set"  : {"state": state},
                             "$push" : {"statehistory": {"state"  : state, "timestamp": ts}},
                             "$push" : {"log"         : {"message": log,   "timestamp": ts}}})

        result = bulk.execute()

        # FIXME: log result.
        # WHY DON'T WE HAVE A LOGGER HERE?


    #--------------------------------------------------------------------------
    #
    def get_compute_unit_states(self, umgr_uid, unit_ids=None):
        """
        Get yerself a bunch of compute units.
        """
        
        if not self._c:
            raise Exception("No active session.")

        if not unit_ids:
            cursor = self._c.find({'type'  : 'unit', 
                                   "umgr"  : umgr_uid},
                                  {"state" : 1}
            )

        else:
            cursor = self._c.find({'type'  : 'unit', 
                                   "uid"   : {"$in": unit_ids},
                                   "umgr"  : umgr_uid},
                                  {"state": 1})

        return [doc['state'] for doc in cursor]


    #--------------------------------------------------------------------------
    #
    def insert_unit_manager(self, umgr_uid, scheduler):
        """ 
        Adds a unit managers to the list of unit managers.
        """
        if not self._c:
            raise Exception("No active session.")

        # FIXME: also store cfg
        result = self._c.insert({'type'      : 'umgr', 
                                 "uid"       : umgr_uid,
                                 "scheduler" : scheduler})
        return result


    #--------------------------------------------------------------------------
    #
    def get_unit_manager(self, umgr_uid):
        """ Get a unit manager.
        """
        if not self._c:
            raise RuntimeError("No active session.")

        cursor = self._c.find({'type' : 'umgr', 
                               "uid"  : umgr_uid})
        try:
            return cursor[0]
        except:
            raise RuntimeError("No UnitManager with id '%s' found in database." % umgr_uid)


    #--------------------------------------------------------------------------
    #
    def get_pilot_manager(self, pmgr_uid):
        """ Get a unit manager.
        """
        if not self._c:
            raise RuntimeError("No active session.")

        cursor = self._c.find({'type' : 'pmgr', 
                               "uid"  : pmgr_uid})
        try:
            return cursor[0]
        except:
            raise RuntimeError("No pilot manager with id '%s' found in DB." % pmgr_uid)


    #--------------------------------------------------------------------------
    #
    def list_unit_manager_uids(self):
        """
        Lists all pilot managers.
        """

        if not self._c:
            raise RuntimeError("No active session.")

        cursor = self._c.find({'type' : 'umgr'})

        return [doc['uid'] for doc in cursor]


    #--------------------------------------------------------------------------
    #
    def unit_manager_add_pilots(self, umgr_uid, pilot_ids):
        """
        Adds a pilot from a unit manager.
        """

        if not self._c:
            raise RuntimeError("No active session.")

        for pilot_uid in pilot_ids:
            self._c.update({'type' : 'pilot', 
                            "uid"  : pilot_uid},
                           {"$set" : {"umgr": umgr_uid}},
                           multi=True)


    #--------------------------------------------------------------------------
    #
    def unit_manager_remove_pilots(self, umgr_uid, pilot_ids):
        """
        Removes one or more pilots from a unit manager.
        """

        if not self._c:
            raise RuntimeError("No active session.")

        # Add the ids to the pilot's queue
        for pilot_uid in pilot_ids:
            self._c.update({'type' : 'pilot', 
                            "uid"  : pilot_uid},
                           {"$set" : {"umgr": None}}, 
                           multi=True)


    #--------------------------------------------------------------------------
    #
    def unit_manager_list_pilots(self, umgr_uid):
        """ 
        Lists all pilots associated with a unit manager.

        Return a list of umgr uids
        """
        if not self._c:
            raise RuntimeError("No active session.")

        cursor = self._c.find({'type' : 'pilot',
                               "umgr" : umgr_uid})

        return [doc['uid'] for doc in cursor]


    #--------------------------------------------------------------------------
    #
    def unit_manager_list_compute_units(self, umgr_uid, pilot_uid=None):
        """ Lists all compute units associated with a unit manager.
        """
        # FIXME: why is this call not updating local unit state?
        if not self._c:
            raise RuntimeError("No active session.")

        if pilot_uid:
            cursor = self._c.find({"type"  : 'unit', 
                                   "umgr"  : umgr_uid, 
                                   "pilot" : pilot_uid})
        else:
            cursor = self._c.find({"type"  : 'unit', 
                                   "umgr"  : umgr_uid})

        return [doc['uid'] for doc in cursor]


    #--------------------------------------------------------------------------
    #
    def pilot_list_compute_units(self, pilot_uid):
        """ Lists all compute units associated with a unit manager.
        """
        # FIXME: why is this call not updating local unit state?
        if not self._c:
            raise RuntimeError("No active session.")

        cursor = self._c.find({'type'  : 'unit', 
                               "pilot" : pilot_uid})

        return [doc['uid'] for doc in cursor]


    #--------------------------------------------------------------------------
    #
    def assign_compute_units_to_pilot(self, units, pilot_uid, pilot_sandbox):
        """Assigns one or more compute units to a pilot.
        """

        if not units:
            return

        if not self._c:
            raise RuntimeError("No active session.")

        # Make sure we work on a list.
        if not isinstance(units, list):
            units = [units]

        bulk = self._c.initialize_ordered_bulk_op()

        for unit in units:

            bulk.find  ({'type' : 'unit', 
                         "uid"  : unit.uid}) \
                .update({"$set" : {"description"   : unit.description.as_dict(),
                                   "pilot"         : pilot_uid,
                                   "pilot_sandbox" : pilot_sandbox,
                                   "sandbox"       : unit.sandbox,
                                   # FIXME: staging directives...
                                  }})
        result = bulk.execute()

        # TODO: log result.
        # WHY DON'T WE HAVE A LOGGER HERE?


    #--------------------------------------------------------------------------
    #
    def publish_compute_unit_callback_history(self, uid, callback_history):

        # FIXME

        if not self._c:
            raise RuntimeError("No active session.")

        self._c.update({'type' : 'unit', 
                        "uid"  : uid},
                       {"$set" : {"callbackhistory": callback_history}})


    #--------------------------------------------------------------------------
    #
    def insert_compute_units(self, umgr_uid, units, unit_log):
        """ 
        Adds one or more compute units to the database.
        """
        if not self._c:
            raise RuntimeError("No active session.")

        # Make sure we work on a list.
        if not isinstance(units, list):
            units = [units]

        unit_docs = list()
        results   = dict()

        for unit in units:

            ts = timestamp()

            unit_json = {
                "uid":                     unit.uid,
                "type":                    'unit',
                "description":             unit.description.as_dict(),
                "restartable":             unit.description.restartable,
                "umgr":                    umgr_uid,
                "pilot":                   None,
                "pilot_sandbox":           None,
                "state":                   unit.state,
                "statehistory":            [{"state": unit.state, "timestamp": ts}],
                "submitted":               ts,
                "started":                 None,
                "finished":                None,
                "exit_code":               None,
                "sandbox":                 None,
                "stdout":                  None,
                "stderr":                  None,
                "log":                     unit_log
            }
            unit_docs.append(unit_json)
            results[unit.uid] = unit_json

        unit_uids = self._c.insert(unit_docs)

        assert len(unit_docs) == len(unit_uids)
        assert len(results)   == len(unit_uids)

        return results


# ------------------------------------------------------------------------------

