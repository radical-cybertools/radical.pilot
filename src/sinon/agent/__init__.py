"""
.. module:: sinon.agent
   :platform: Unix
   :synopsis: The agent part of sinon.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

from exception import AgentException
from task_queue import Task, TaskQueue
from result_queue import Result, ResultQueue
from task_executor import TaskExecutor
from execution_environment import ExecutionEnvironment
