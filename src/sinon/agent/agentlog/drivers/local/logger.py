#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"


''' Provides log handler management for RHYTHMOS.
'''

import logging

from radical.utils import Singleton

from filehandler import FileHandler
from defaultformatter import DefaultFormatter
from colorstreamhandler import ColorStreamHandler, has_color_stream_handler


################################################################################
##
class _Logger():
    """
    :todo: documentation.  Also, documentation of options are insufficient
    (like, what are valid options for 'target'?)

    This class is not to be directly used by applications.
    """

    __metaclass__ = Singleton

    class _MultiNameFilter(logging.Filter):
        def __init__(self, pos_filters, neg_filters=[]):
            self._pos_filters = pos_filters
            self._neg_filters = neg_filters

        def filter(self, record):
            print_it = False

            if not len(self._pos_filters) :
                print_it = True
            else :
                for f in self._pos_filters:
                    if  f in record.name:
                        print_it = True

            for f in self._neg_filters:
                if  f in record.name:
                    print_it = False

            return print_it

    def __init__(self):

        self._loglevel = "4"
        self._filters  = []
        self._targets  = ["stdout"]
        self._handlers = list()

        if self._loglevel is not None:
            if self._loglevel.isdigit():
                if   int(self._loglevel)    >= 4:           self._loglevel = logging.DEBUG
                elif int(self._loglevel)    == 3:           self._loglevel = logging.INFO
                elif int(self._loglevel)    == 2:           self._loglevel = logging.WARNING
                elif int(self._loglevel)    == 1:           self._loglevel = logging.ERROR
                elif int(self._loglevel)    == 0:           self._loglevel = logging.CRITICAL
                else: raise saga.exceptions.NoSuccess('%s is not a valid value for SAGA_VERBOSE.' % self._loglevel)
            else:
                if   self._loglevel.lower() == 'debug':     self._loglevel = logging.DEBUG
                elif self._loglevel.lower() == 'info':      self._loglevel = logging.INFO
                elif self._loglevel.lower() == 'warning':   self._loglevel = logging.WARNING
                elif self._loglevel.lower() == 'error':     self._loglevel = logging.ERROR
                elif self._loglevel.lower() == 'critical':  self._loglevel = logging.CRITICAL
                else: raise saga.exceptions.NoSuccess('%s is not a valid value for SAGA_VERBOSE.' % self._loglevel)

        # create the handlers (target + formatter + filter)
        for target in self._targets:

            if target.lower() == 'stdout':
                # create a console stream logger
                # Only enable colour if support was loaded properly
                if has_color_stream_handler is True:
                    handler = ColorStreamHandler()
                else: 
                    handler = logging.StreamHandler()
            else:
                # got to be a file logger
                handler = FileHandler(target)

            handler.setFormatter(DefaultFormatter)

            if self._filters != []:
                pos_filters = []
                neg_filters = []

                for f in self._filters :
                    if  f and f[0] == '!' :
                        neg_filters.append (f[1:])
                    else :
                        pos_filters.append (f)

                handler.addFilter(self._MultiNameFilter (pos_filters, neg_filters))

            self._handlers.append(handler)


    @property
    def loglevel(self):
        return self._loglevel

    @property
    def handlers(self):
        return self._handlers


################################################################################
#
# FIXME: strange pylint error
#
def getLogger (suffix):
    ''' Get a SAGA logger.  For any new name, a new logger instance will be
    created; subsequent calls to this method with the same name argument will
    return the same instance.'''

    name = 'radical.agent'
    if suffix is not None:
        name += '.%s' % suffix

    # make sure the saga logging configuration is evaluated at least once
    _Logger ()

    # get a python logger
    _logger = logging.getLogger(name)

    # was this logger initialized before?
    #
    # The check / action below forms a race condition, but (a) this should be
    # rare (TM), and (b) it does have no practical consequences (apart from a 
    # small runtime overhead).  So, we don't care... :-P
    if _logger.handlers == []:

        # initialize that logger
        for handler in _Logger().handlers:
            _logger.addHandler(handler)

        _logger.setLevel(_Logger().loglevel)
        _logger.propagate = 0 # Don't bubble up to the root logger

    
    # setup done - we can return the logger
    return _logger

