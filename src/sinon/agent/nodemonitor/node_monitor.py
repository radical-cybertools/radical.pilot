#!/user/bin/env python
# -*- coding: utf-8 -*-

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__email__     = "ole.weidner@icloud.com"
__license__   = "MIT"


import json
import time
import psutil
import socket
import datetime
import os.path 

UPDATE_INTERVAL=5

#-----------------------------------------------------------------------------
#
class NodeMonitor(object):

    def __init__(self, workdir):
        ''' Le constructeur.
        '''
        self._workdir = workdir
        self._nodename = socket.gethostname()

        ioc = psutil.disk_io_counters()
        self._prev_io_read_bytes = ioc.read_bytes
        self._prev_io_write_bytes = ioc.write_bytes

        nioc = psutil.net_io_counters()
        self._prev_net_bytes_recv = nioc.bytes_recv
        self._prev_net_bytes_sent = nioc.bytes_sent

    def write(self):

        mem =  psutil.virtual_memory()
        cpu =  psutil.cpu_times_percent()
        ioc =  psutil.disk_io_counters()
        nioc = psutil.net_io_counters()

        if hasattr(cpu, 'iowait'):
            cpu_iowait = cpu.iowait
        else:
            cpu_iowait = 0

        bytes_written = (ioc.write_bytes - self._prev_io_write_bytes) / UPDATE_INTERVAL
        bytes_read = (ioc.read_bytes - self._prev_io_read_bytes) / UPDATE_INTERVAL


        bytes_sent = (nioc.bytes_sent - self._prev_net_bytes_sent) / UPDATE_INTERVAL
        bytes_recv = (nioc.bytes_recv - self._prev_net_bytes_recv) / UPDATE_INTERVAL

        nodestats = {
            'type'                : 'nodestats',
            'update_interval'     : UPDATE_INTERVAL,
            'timestamp'           : datetime.datetime.now().strftime("%y-%m-%d %X"),
            'nodename'            : self._nodename,
            'mem_free'            : mem.percent,
            'cpu_user'            : cpu.user,
            'cpu_nice'            : cpu.nice,
            'cpu_system'          : cpu.system,   
            'cpu_idle'            : cpu.idle,
            'cpu_iowait'          : cpu_iowait,
            'io_read_count'       : ioc.read_count,
            'io_write_count'      : ioc.write_count,
            'io_read_bytes'       : ioc.read_bytes,
            'io_write_bytes'      : ioc.write_bytes,
            'io_read_throughput'  : bytes_read,
            'io_write_throughput' : bytes_written,
            'net_recv_bytes'      : nioc.bytes_recv,
            'net_sent_bytes'      : nioc.bytes_sent,
            'net_recv_throughput' : bytes_recv,
            'net_send_throughput' : bytes_sent
        }

        with open('%s/nodestats.json' % self._workdir, 'a') as outfile:
            outfile.write(json.dumps(nodestats)+'\n')


    def run(self):
        while True:
            time.sleep(UPDATE_INTERVAL)
            self.write()

            if os.path.exists("%s/TERMINATE" % self._workdir):
                print "Found 'TERMINATE' -- exiting..."
                return 0
