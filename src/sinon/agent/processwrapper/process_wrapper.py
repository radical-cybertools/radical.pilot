#!/user/bin/env python
# -*- coding: utf-8 -*-

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__email__     = "ole.weidner@icloud.com"
__license__   = "MIT"


import json
import socket
import time
import psutil
import threading 
import datetime
import subprocess


#-----------------------------------------------------------------------------
#
class WatchDogThread(threading.Thread):


    #-------------------------------------------------------------------------
    #
    def __init__(self, pid, interval, outputfile):
        threading.Thread.__init__(self)
        self._pid        = pid
        self._interval   = interval
        self._outputfile = outputfile

        self._nodename = socket.gethostname()


    #-------------------------------------------------------------------------
    #
    def write(self, mem, cpu, ioc):

        #bytes_written = (ioc.write_bytes - self._prev_io_write_bytes) / UPDATE_INTERVAL
        #bytes_read = (ioc.read_bytes - self._prev_io_read_bytes) / UPDATE_INTERVAL

        nodestats = {
            'type'           : 'procstats',
            'update_interval': self._interval,
            'timestamp'      : datetime.datetime.now().strftime("%y-%m-%d %X"),
            'nodename'       : self._nodename,
            'mem_rss'        : mem.rss,
            'mem_vms'        : mem.vms,
            'cpu_user'       : cpu.user,
            'cpu_system'     : cpu.system,   
            'io_read_count'  : ioc.read_count,
            'io_write_count' : ioc.write_count,
            'io_read_bytes'  : ioc.read_bytes,
            'io_write_bytes' : ioc.write_bytes,
            'io_read_throughput'  : bytes_read,
            'io_write_throughput' : bytes_written
        }

    #-------------------------------------------------------------------------
    #
    def run(self):

        try:
            # this is the process handle
            p = psutil.Process(self._pid)

            # check if platform supports I/O counters
            try: 
                ioc = p.get_io_counters()
                supports_io_counters = True
                self._prev_io_write_bytes = ioc.write_bytes
                self._prev_io_read_bytes = ioc.read_bytes

            except AttributeError:
                supports_io_counters = False
                self._prev_io_write_bytes = 0
                self._prev_io_read_bytes = 0

            # the output file:
            outfile = open('./%s' % self._outputfile, 'a')

            # the big loop
            while p.is_running():

                cpu_percent = p.get_cpu_percent(self._interval)
                proc_mem    = p.get_memory_info()

                if supports_io_counters:
                    ioc = p.get_io_counters()
                    bytes_written = (ioc.write_bytes - self._prev_io_write_bytes) / self._interval
                    bytes_read = (ioc.read_bytes - self._prev_io_read_bytes) / self._interval

                    ioc_dict = {
                        'io_read_count'  : ioc.read_count,
                        'io_write_count' : ioc.write_count,
                        'io_read_bytes'  : ioc.read_bytes,
                        'io_write_bytes' : ioc.write_bytes,
                        'io_read_throughput'  : bytes_read,
                        'io_write_throughput' : bytes_written
                    }

                nodestats = {
                    'type'           : 'procstats',
                    'pid'            : self._pid,
                    'update_interval': self._interval,
                    'timestamp'      : datetime.datetime.now().strftime("%y-%m-%d %X"),
                    'nodename'       : self._nodename,
                    'mem_rss'        : proc_mem.rss,
                    'mem_vms'        : proc_mem.vms,
                    'cpu_percent'    : cpu_percent,
                }

                if supports_io_counters:
                    nodestats.update(ioc_dict)

                outfile.write(json.dumps(nodestats)+'\n')

        except psutil.NoSuchProcess, pex:
            print pex
            return 0
        except psutil.AccessDenied, pex:
            print pex
            return 0
        finally:
            outfile.close()


#-----------------------------------------------------------------------------
#
class ProcessWrapper(object):

    def __init__(self, interval, outputfile, args):
        ''' Le constructeur.
        '''
        self._interval   = interval
        self._outputfile = outputfile
        self._args       = args


    def run(self):
        try:
            p = subprocess.Popen(self._args)
            pid = p.pid

            print "Process PID %s" % pid
            wd = WatchDogThread(pid, self._interval, self._outputfile)
            wd.start()
            p.communicate() # waits for process to finish
            print "Process RC %s" % p.returncode
            wd.join()
        except OSError, ex:
            logger.error("Error running "+str(cmdline)+": "+str(ex))
