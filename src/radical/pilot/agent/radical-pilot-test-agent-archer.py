#!/usr/bin/env python

"""
.. module:: radical.pilot.agent
   :platform: Unix
   :synopsis: An agent "skeleton" for RADICAL-Pilot. 

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import ast
import sys
import time
import errno
import pipes
import Queue
import signal
import gridfs
import pymongo
import optparse
import select
import socket
import logging
import datetime
import hostlist
import traceback
import threading 
import subprocess
import multiprocessing

from bson.objectid import ObjectId

#--------------------------------------------------------------------------
# Configure the logger
LOGGER = logging.getLogger('radical.pilot.agent')
LOGGER.setLevel(logging.INFO)
ch = logging.FileHandler("AGENT.LOG")
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
LOGGER.addHandler(ch)

#---------------------------------------------------------------------------
#
def pilot_FAILED(mongodb_handle, pilot_uid, message):
    """Updates the state of one or more pilots.
    """
    pilot_collection = mongo_db["%s.p"  % options.session_id]

    LOGGER.error(message)      
    ts = datetime.datetime.utcnow()

    pilot_collection.update({"_id": ObjectId(pilot_uid)}, 
        {"$push": {"log" : message,
                   "statehistory": {"state": 'Failed', "timestamp": ts}},
         "$set":  {"state": 'Failed',
                   "finished": ts}

        })

#---------------------------------------------------------------------------
#
def pilot_FAILED(mongodb_handle, pilot_uid, message):
    """Updates the state of one or more pilots.
    """
    pilot_collection = mongo_db["%s.p"  % options.session_id]

    LOGGER.error(message)      
    ts = datetime.datetime.utcnow()

    pilot_collection.update({"_id": ObjectId(pilot_uid)}, 
        {"$push": {"log" : message,
                   "statehistory": {"state": 'Failed', "timestamp": ts}},
         "$set":  {"state": 'Failed',
                   "finished": ts}

        })

#---------------------------------------------------------------------------
#
def pilot_CANCELED(mongodb_handle, pilot_uid, message):
    """Updates the state of one or more pilots.
    """
    pilot_collection = mongo_db["%s.p"  % options.session_id]

    LOGGER.warning(message)
    ts = datetime.datetime.utcnow()

    pilot_collection.update({"_id": ObjectId(pilot_uid)}, 
        {"$push": {"log" : message,
                   "statehistory": {"state": 'Canceled', "timestamp": ts}},
         "$set":  {"state": 'Canceled',
                   "finished": ts}
        })

#---------------------------------------------------------------------------
#
def pilot_DONE(mongodb_handle, pilot_uid, message):
    """Updates the state of one or more pilots.
    """
    pilot_collection = mongo_db["%s.p"  % options.session_id]

    LOGGER.info(message)
    ts = datetime.datetime.utcnow()

    pilot_collection.update({"_id": ObjectId(pilot_uid)}, 
        {"$push": {"log" : message,
                   "statehistory": {"state": 'Done', "timestamp": ts}},
         "$set": {"state": 'Done',
                  "finished": ts}
        })


# ----------------------------------------------------------------------------
#
class Agent(threading.Thread):

    # ------------------------------------------------------------------------
    #
    def __init__(self, project, workdir, cores, runtime, mongodb_url, mongodb_name, pilot_id, session_id):
        """Le Constructeur creates a new Agent instance.
        """
        threading.Thread.__init__(self)
        self.daemon      = True
        self.lock        = threading.Lock()
        self._terminate  = threading.Event()

        self._project    = project
        self._workdir    = workdir
        self._pilot_id   = pilot_id
        self._cores      = cores

        self._runtime    = runtime
        self._starttime  = None

        mongo_client = pymongo.MongoClient(mongodb_url)
        self.mongo_db = mongo_client[mongodb_name]
        self.pilot_collection = self.mongo_db["%s.p"  % session_id]
        self.computeunit_collection = self.mongo_db["%s.w"  % session_id]

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """Terminate the agent main loop.
        """
        # Next, we set our own termination signal
        self._terminate.set()

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the thread when Thread.start() is called.
        """
        # first order of business: set the start time and state of the pilot
        LOGGER.info("Agent started. Database updated.")
        ts = datetime.datetime.utcnow()

        # ---------------------------------
        # Update the pilot's database entry
        self.pilot_collection.update(
            {"_id": ObjectId(self._pilot_id)}, 
            {"$set": {"state"          : "Running",
                      "nodes"          : "SKEL-AGENT-None",
                      "cores_per_node" : "SKEL-AGENT-None",
                      "started"        : ts},
             "$push": {"statehistory": {"state": 'Running', "timestamp": ts}}
            })

        self._starttime = time.time()
        #####################################
        # START
        #####################################
        HOSTNAME = socket.gethostname()
        LOGGER.info("AGENT HOSTNAME: %s" % HOSTNAME)
        HOST = socket.gethostbyname(HOSTNAME)
        LOGGER.info("AGENT HOST: %s" % HOST)
     
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((HOST, 0))
        PORT = server.getsockname()[1]
        LOGGER.info("AGENT USES PORT: %s" % PORT)
                    
        # this should be defined somewhere?
        # for archer we define 24 cores per node
        #ARCHER_NODE = 24
        # for localhost we use 2 cores 
        ARCHER_NODE = 2
        # determining how many nodes to allocate with agent-worker.py script
        if( int(self._cores) < ARCHER_NODE ):
            NODES = 1
        else:
            NODES = int(self._cores) / ARCHER_NODE
            if ( (int(self._cores) % ARCHER_NODE) > 0 ):
                NODES += 1

        ################################################################################################
        # opening agent-worker.py file in order to pass IP address, port number, number of nodes, and walltime
        ################################################################################################
        LOGGER.info("WRITING AGENT'S ADDRESS AND PORT NUMBER TO agent-worker.py FILE")
        name = 'agent-worker.py'

        try:
            rfile = open(name,'r')
        except IOError:
            LOGGER.info("WARNING UNABLE TO ACCESS FILE: %s" % name)
                        
        tbuffer = rfile.read()
        rfile.close()

        tbuffer = tbuffer.replace("@project@",str(self._project))
        tbuffer = tbuffer.replace("@server@",str(HOST))
        tbuffer = tbuffer.replace("@port@",str(PORT))
        tbuffer = tbuffer.replace("@select@",str(NODES))
        tbuffer = tbuffer.replace("@walltime@",self._runtime)

        try:
            wfile = open(name,'w')
        except IOError:
            LOGGER.info("WARNING UNABLE TO ACCESS FILE: %s" % name)

        wfile.write(tbuffer)
        wfile.close()
        LOGGER.info("FINISHED WRITING TO agent-worker.py...")
        #####################################################
        FINISH = 'STOP'
        WAIT = 'WAIT'
        LOGGER.info("CALLING QSUB...")
        # agent submits agent-worker.py using qsub on archer from work file system
        # currently this is commented out since we can only run on localhost
        # proc = subprocess.Popen(["qsub agent-worker.py"], stdout=subprocess.PIPE, shell=True)

        # line below is for localhost execution only!
        proc = subprocess.Popen(["nohup python agent-worker.py"], stdout=subprocess.PIPE, shell=True)
        LOGGER.info("QSUB CALL SUCCEEDED...")
        server.listen(32)
        LOGGER.info("AGENT STARTED LISTENING AT %s" % HOST)
        input = [server,]
        #####################################
        # This is the main thread loop
        #####################################
        while True:
            try:
                # Exit the main loop if terminate is set. 
                if self._terminate.isSet():
                    pilot_CANCELED(self.mongo_db, self._pilot_id, "Agent terminated.")
                    break

                # Make sure that we haven't exceeded the agent runtime. if 
                # we have, terminate. 
                if time.time() >= self._starttime + (int(self._runtime) * 60):
                    runtime_limit = str(int(self._runtime)*60)
                    message = "Agent has reached its runtime limit (%s seconds)." % runtime_limit
                    pilot_DONE(self.mongo_db, self._pilot_id, message)
                    break

                # Check if there's a command waiting.
                cursor = self.pilot_collection.find({"_id": ObjectId(self._pilot_id)})
                command = cursor[0]['command']
                if command is not None:
                    if command.lower() == "cancel":
                        pilot_CANCELED(self.mongo_db, self._pilot_id, "Command 'CANCEL' received. Terminating.")
                        break
                    else:
                        LOGGER.warning("Received unknown command '%s'." % command )

                # Check if there are work units waiting for execution
                ts = datetime.datetime.utcnow()

                computeunits = self.computeunit_collection.find_and_modify(
                query={"pilot" : self._pilot_id,
                       "state" : "PendingExecution"},
                update={"$set" : {"state": "Running"},
                "$push": {"statehistory": {"state": "RunningX", "timestamp": ts}}}
                #limit=BULK_LIMIT
                )

                # There are new work units in the wu_queue on the database.
                # Get the corresponding wu entries
                if computeunits is not None:
                    if not isinstance(computeunits, list):
                        computeunits = [computeunits]

                    #######################################
                    # initialize params
                    #######################################
                    aprun_tasks = []
                    free_nodes = NODES
                    LOGGER.info("INIT FREE_NODES: %s" % free_nodes)
                    #######################################
                    for cu in computeunits:
                        LOGGER.info("Processing ComputeUnit: %s" % cu)
                    
                        # Create the task working directory if it doesn't exist
                        cu_workdir = "%s/unit-%s" % (self._workdir, str(cu["_id"]))
                        if not os.path.exists(cu_workdir):
                            os.makedirs(cu_workdir)

                        # Create bogus STDOUT and STDERR
                        open("%s/STDOUT" % cu_workdir, 'a').close()
                        open("%s/STDERR" % cu_workdir, 'a').close()
                       
                        ##############################################
                        # below is aprun string for archer, currenty commented out
                        # cu_str = "aprun -n %s %s > %s" % (cu['description']['cores'], cu['description']['executable'], cu_workdir + "/STDOUT")
                        
                        w_dir = cu_workdir + "/STDOUT"
                        # this is for localhost execution only!
                        cu_str = "date > %s" % w_dir
     
                        LOGGER.info("CU_STR: %s" % cu_str)
                        if( int(cu['description']['cores']) < ARCHER_NODE ):
                            cu_nodes = 1
                        else:
                            cu_nodes = int(cu['description']['cores']) / ARCHER_NODE
                            if ( (int(cu['description']['cores']) % ARCHER_NODE) > 0 ):
                                cu_nodes += 1
   
                        LOGGER.info("CU_NODES: %s" % cu_nodes)
                        aprun_tasks.append(cu_str)
                        free_nodes = free_nodes - cu_nodes
                        LOGGER.info("AFTER 1 CU FREE_NODES: %s" % free_nodes)
                        ##############################################
                        # SERVER BLOCK
                        ##############################################
                        run = 1
                        if (free_nodes < 1):
                            while (run >= 0):
                                inputready,outputready,exceptready = select.select(input,[],[])
                                for s in inputready:
                                    if s == server:
                                        # handle the server socket 
                                        client, address = server.accept()
                                        input.append(client)
                                        LOGGER.info("AGENT WORKER ADDED: %s" % str(address))
                                    else:
                                        # handle all other sockets 
                                        data = s.recv(1024)
                                        LOGGER.info("AGENT RECEIVED FROM AGENT WORKER: %s" % repr(data))
                                        if (run > 0):
                                            aprun_str = ""
                                            for task in aprun_tasks:
                                                if aprun_str == "":
                                                    aprun_str = task
                                                else:
                                                    aprun_str = aprun_str + "&" + task                                
                                            LOGGER.info("AGENT IS SENDING EXECUTION STRING...")
                                            s.sendall(aprun_str)
                                            run -= 1
                                        else:
                                            s.sendall(WAIT)
                                            run -= 1    
                        ##############################################
                        # SERVER BLOCK ENDS
                        ##############################################
                        if cu['description']['output_data'] is not None:
                            state = "PendingOutputTransfer"
                        else:
                            state = "Done"

                        self.computeunit_collection.update({"_id": cu["_id"]}, 
                            {"$set": {"state"         : state,
                                      "started"       : "SKEL-AGENT-None",
                                      "finished"      : "SKEL-AGENT-None",
                                      "exec_locs"     : "SKEL-AGENT-None",
                                      "exit_code"     : "SKEL-AGENT-None",
                                      "stdout_id"     : "SKEL-AGENT-None",
                                      "stderr_id"     : "SKEL-AGENT-None"},
                             "$push": {"statehistory": {"state": state, "timestamp": ts}}
                            })

            except Exception, ex:
                # If we arrive here, there was an exception in the main loop.
                pilot_FAILED(self.mongo_db, self._pilot_id, 
                    "ERROR in agent main loop: %s. %s" % (str(ex), traceback.format_exc()))
                return 

        #########################
        # MAIN LOOP TERMINATED
        #########################
        LOGGER.info("AGENT IN TERMINATING CONNECTION TO AGENT WORKER...")
        s.sendall(FINISH)  
        s.close()
        input.remove(s)
        server.close()
        #########################
        pilot_DONE(self.mongo_db, self._pilot_id, "Pilot main loop completed.")
        return

# ================================================================================
# ================================================================================
#
# BELOW THIS LINE NOTHING NEEDS TO BE TOUCHED (I THINK). IT'S MOSTLY SIGNAL 
# HANDLING STUFF, COMMAND-LINE PARSING, ETC.

def parse_commandline():

    parser = optparse.OptionParser()

    parser.add_option('-d', '--mongodb-url',
                      metavar='URL',
                      dest='mongodb_url',
                      help='Specifies the MongoDB Url.')

    parser.add_option('-n', '--database-name',
                      metavar='URL',
                      dest='database_name',
                      help='Specifies the MongoDB database name.')

    parser.add_option('-s', '--session-id',
                      metavar='SID',
                      dest='session_id',
                      help='Specifies the Session ID.')

    parser.add_option('-p', '--pilot-id',
                      metavar='PID',
                      dest='pilot_id',
                      help='Specifies the Pilot ID.')

    parser.add_option('-w', '--workdir',
                      metavar='DIRECTORY',
                      dest='workdir',
                      help='Specifies the base (working) directory for the agent. [default: %default]',
                      default='.')

    parser.add_option('-c', '--cores',
                      metavar='CORES',
                      dest='cores',
                      help='Specifies the number of cores to allocate.')

    parser.add_option('-t', '--runtime',
                      metavar='RUNTIME',
                      dest='runtime',
                      help='Specifies the agent runtime in minutes.')

    parser.add_option('-l', '--launch-method', 
                      metavar='METHOD',
                      dest='launch_method',
                      help='Enforce a specific launch method (AUTO, LOCAL, SSH, MPIRUN, APRUN). [default: %default]',
                      default="AUTO")

    parser.add_option('-V', '--version', 
                      metavar='VERSION ',
                      dest='package_version',
                      help='The RADICAL-Pilot package version.')

    parser.add_option('-a', '--allocation',
                      metavar='ALLOCATION',
                      dest='project',
                      help='Specifies project code.')

    # parse the whole shebang
    (options, args) = parser.parse_args()

    if options.mongodb_url is None:
        parser.error("You must define MongoDB URL (-d/--mongodb-url). Try --help for help.")
    elif options.database_name is None:
        parser.error("You must define a database name (-n/--database-name). Try --help for help.")
    elif options.session_id is None:
        parser.error("You must define a session id (-s/--session-id). Try --help for help.")
    elif options.pilot_id is None:
        parser.error("You must define a pilot id (-p/--pilot-id). Try --help for help.")
    elif options.cores is None:
        parser.error("You must define the number of cores (-c/--cores). Try --help for help.")
    elif options.runtime is None:
        parser.error("You must define the agent runtime (-t/--runtime). Try --help for help.")
    elif options.package_version is None:
        parser.error("You must pass the RADICAL-Pilot package version (-v/--version). Try --help for help.")
    elif options.project is None:
        parser.error("You must pass your project's allocation code (-a/--allocation). Try --help for help.")


    #if options.launch_method is not None: 
    #    valid_options = [LAUNCH_METHOD_AUTO, LAUNCH_METHOD_LOCAL, LAUNCH_METHOD_SSH, LAUNCH_METHOD_MPIRUN, LAUNCH_METHOD_APRUN]
    #    if options.launch_method.upper() not in valid_options:
    #        parser.error("--launch-method must be one of these: %s" % valid_options)

    return options

#-----------------------------------------------------------------------------
#
if __name__ == "__main__":

    # parse command line options
    options = parse_commandline()

    LOGGER.info("RADICAL-Pilot agent (radical-pilot-agent-skeleton.py) for package/API version %s" % options.package_version)


    #--------------------------------------------------------------------------
    # Establish database connection
    try:
        mongo_client = pymongo.MongoClient(options.mongodb_url)
        mongo_db     = mongo_client[options.database_name]

    except Exception, ex:
        LOGGER.error("Couldn't establish database connection: %s" % str(ex))
        sys.exit(1)

    #--------------------------------------------------------------------------
    # Some singal handling magic 
    def sigint_handler(signal, frame):
        msg = 'Caught SIGINT. EXITING.'
        pilot_CANCELED(mongo_db, options.pilot_id, msg)
        sys.exit(0)
    signal.signal(signal.SIGINT, sigint_handler)

    def sigalarm_handler(signal, frame):
        msg = 'Caught SIGALRM (Walltime limit reached?). EXITING'
        pilot_CANCELED(mongo_db, options.pilot_id, msg)
        sys.exit(0)
    signal.signal(signal.SIGALRM, sigalarm_handler)

    #--------------------------------------------------------------------------
    # Launch the agent thread
    try:
        if options.workdir is '.':
            workdir = os.getcwd()
        else:
            workdir = options.workdir

        agent = Agent(project=options.project,
                      workdir=workdir,
                      cores=options.cores,
                      runtime=options.runtime,
                      mongodb_url=options.mongodb_url,
                      mongodb_name=options.database_name,
                      pilot_id=options.pilot_id,
                      session_id=options.session_id)

        agent.start()
        agent.join()
        sys.exit(0)

    except Exception, ex:
        msg = "Error during agent execution: %s" % str(ex)
        LOGGER.error(msg)
        pilot_FAILED(mongo_db, options.pilot_id, msg)
        agent.stop()
        sys.exit(1)
