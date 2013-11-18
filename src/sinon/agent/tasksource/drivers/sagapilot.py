#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

import os
import json
from sinon.agent import Task

DRIVER = "SASGAPilot"

#-----------------------------------------------------------------------------
#
class SAGAPilot(object):


    #-------------------------------------------------------------------------
    #
    def __init__(self, logger, task_source_url):
        """
        """
        # # extract the file path and open the file
        # # and read the task data
        # self._tasks = list()
        # self.log = logger

        # # try to open the file in the task_source_url
        # task_file_path =task_source_url.path

        # # read JSON task descriptions line by line
        # task_count = 0

        # task_file_name = os.path.basename(task_file_path)
        
        # json_data=open(task_file_path)
        # data = json.load(json_data)

        # # the container type always has to be a list '[]'
        # if type(data) is not list:
        #     raise Exception("In file %s: top-level JSON element must be of type list" % task_file_name)

        # # entries in a list container can be either of type 'task' or 'taskset'
        # for entry in data:
        #     if type(entry) is not dict:
        #         raise Exception("In file %s: malformed entry: %s" % (task_file_name, entry))
        #     try:
        #         entry_type = entry['type']
        #     except Exception, ex:
        #         raise Exception("In file %s: entry doesn't define 'type': %s" % (task_file_name, entry))

        #     # distinguish between types:
        #     if entry_type == 'task':
        #         task_count += 1

        #         # convert entry into a Task object
        #         task = Task(uid=entry['id'],
        #                     executable=entry['executable'],
        #                     arguments=entry['arguments'],
        #                     numcores=entry['requirements']['cores'],
        #                     stdout=None,
        #                     stderr=None)

        #         self._tasks.append(task)

        #     elif entry_type == 'taskset':
        #         # task set defines multiple tasks via a template and 'instances'
        #         for i in range(1, int(entry['instances'])+1):
        #             task_count += 1
        #             template = entry['template']
        #             # convert template task into a Task object
        #             task = Task(uid="%s%s" % (template['id'], str(i)),
        #                         executable=template['executable'],
        #                         arguments=template['arguments'],
        #                         numcores=template['requirements']['cores'],
        #                         stdout=None,
        #                         stderr=None)

        #             self._tasks.append(task)

        #     else:
        #         raise Exception("In file %s: unkown 'type': %s" % (task_file_name, entry))


        # self.log.info("%s: Successfully opened file '%s'" % (DRIVER, task_file_path))
        # self.log.info("%s: Loaded %s task descriptions from file into memory" % (DRIVER, task_count))


    #-------------------------------------------------------------------------
    #
    def is_finite(self):
        # file sources are always finite
        return True

    #-------------------------------------------------------------------------
    #
    def num_tasks(self):
        """
        """
        return 0

    #-------------------------------------------------------------------------
    #
    def get_tasks(self, start_idx=0, limit=None):
        return []

    #-------------------------------------------------------------------------
    #
    def close(self):
        pass
