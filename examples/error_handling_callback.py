#!/usr/bin/env python

""" LINK TO TUTORIAL PAGE
"""

__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import sinon
import time

PWD    = os.path.dirname(os.path.abspath(__file__))
DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'file://localhost/%s/../../configs/futuregrid.json' % PWD
