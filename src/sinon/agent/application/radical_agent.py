#!/user/bin/env python
# -*- coding: utf-8 -*-

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__email__     = "ole.weidner@icloud.com"
__license__   = "MIT"

import os
import uuid
import signal
import Queue

import sinon.frontend.states as states

from sinon.agent import Task, TaskQueue, TaskExecutor, ExecutionEnvironment

from sinon.agent.agentlog import AgentLog
from sinon.agent.tasksource import TaskSource
from sinon.agent.taskevents import TaskEvents
from sinon.agent.taskresults import TaskResults


#-----------------------------------------------------------------------------
#
class RhythmosAgent(object):

    #-------------------------------------------------------------------------
    #
    def __init__(self, agent_log, workdir, tasks_per_node, 
                 dedicated_master_node, detect_node_failure, launch_method):
        ''' Le constructeur.
        '''

        self._task_source_url  = None
        self.task_source       = None

        self._task_results_url = None
        self.task_results      = None

        self._task_events_url  = None
        self.task_events       = None

        self._task_metrics_url = None
        self.task_metrics      = None

        # initialize agent logger
        self._agent_log_url = agent_log
        self.log = AgentLog(agent_log)

        # The working directory for this agent
        self._workdir = os.path.abspath(workdir)

        # Max. number of tasks to run on a single node concurrently
        self._tasks_per_node = tasks_per_node

        # Explicit launch method or 'AUTO' for autodetection
        self._launch_method = launch_method

        # wether to detect and mitigate faulty nodes
        self._detect_node_failure = detect_node_failure

        # wether to use one node just for task coordination
        self._dedicated_master_node = dedicated_master_node

        # Create working directory in case it doesn't exist
        if not os.path.exists(self._workdir):
            os.makedirs(self._workdir)
        self.log.info("Rhythmos Agent starting up in working directory '%s'" % self._workdir)


    #-------------------------------------------------------------------------
    #
    def _init_task_source(self, task_source_url):
        # try to load a task source driver for the task source URL
        # that passed to 'run()'
        ts = TaskSource(logger=self.log,
                        task_source_url=task_source_url)
        self._task_source_url = task_source_url
        return ts

    #-------------------------------------------------------------------------
    #
    def _init_task_results(self, task_results_url):
        # try to load a task source driver for the task source URL
        # that passed to 'run()'
        tr = TaskResults(logger=self.log,
                         task_results_url=task_results_url)
        self._task_results_url = task_results_url
        return tr

    #-------------------------------------------------------------------------
    #
    def _init_task_events(self, task_events_url):
        # try to load a task source driver for the task source URL
        # that passed to 'run()'
        tr = TaskEvents(logger=self.log,
                        task_events_url=task_events_url)
        self._task_events_url = task_events_url
        return tr

    #-------------------------------------------------------------------------
    #
    def run(self, task_source, task_results, task_events, task_metrics):
        ''' The main run() method. This causes the agent to start
            working. This method will block until the agent has finished or
            received a signal to terminate.
        '''
        # Try to load a driver for the task source
        self.task_source = self._init_task_source(task_source)
        # Try to load a driver for task results
        self.task_results = self._init_task_results(task_results)
        # Try tp load a dirver for task events
        self.task_events = self._init_task_events(task_events)

        # All drivers loaded. Try to set the status to 'RUNNING'.
        self.task_events.put(origin_type="AGENT", origin_id=None, 
                event="STATECHANGE", value=states.RUNNING)
        #---------------------------------------------------------------------
        #
        global event_h
        event_h = self.task_events

        def _signal_handler(signum, frame):
            print 'Signal handler called with signal', signum
            event_h.put(origin_type="AGENT", origin_id=None, 
                event="STATECHANGE", value=states.CANCELED)

        # install a signal handler. in theory, this should get triggered
        # if we get kicked out by the queueing system, etc.
        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
            signal.signal(sig, _signal_handler)
        #
        #---------------------------------------------------------------------

        # Discover the execution environment
        ee = ExecutionEnvironment.discover(logger=self.log)

        # Next, we need to connect task source, results and events with the
        # task execution mechanisms

        tq = TaskQueue()

        for task in self.task_source.get_tasks():
            tq.put(task)

        # The event_callback forwards events from the executor to the driver
        def event_callback(origin_type, origin_id, event, value):
            self.task_events.put(origin_type=origin_type, origin_id=origin_id, 
                event=event, value=value)

        # The result_callback forwards results from the executor to the driver
        def result_callback(task_id, cmdline, exit_code, working_directory,
                            stdout, stderr, start_time, stop_time, runtime, execution_locations):
            self.task_results.put(task_id, cmdline, exit_code, working_directory,
                                  stdout, stderr, start_time, stop_time, runtime, execution_locations)

        te = TaskExecutor(task_queue=tq,
                          working_directory=self._workdir,
                          execution_environment=ee,
                          tasks_per_node=self._tasks_per_node,
                          terminate_on_emtpy_queue=True,
                          event_callback=event_callback,
                          result_callback=result_callback,
                          logger=self.log,
                          launch_method=self._launch_method
                          )
        te.start()
        te.join()

        # Finally, set the status to 'DONE'.
        # All drivers loaded. Try to set the status to 'RUNNING'.
        self.task_events.put(origin_type="AGENT", origin_id=None, 
                event="STATECHANGE", value=states.DONE)
