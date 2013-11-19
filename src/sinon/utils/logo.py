"""
.. module:: sinon.utils.logo
   :platform: Unix
   :synopsis: Some nonsense ASCII art.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, radical.rutgers.edu"
__license__   = "MIT"

from sinon import version
from colorama import init, Fore, Back, Style


#-----------------------------------------------------------------------------
#
def print_startup_logo(component_name):
    init()
    logo = Fore.BLUE
    logo += """         _                
   _____(_)___  ____  ____ 
  / ___/ / __ \/ __ \/ __ \\
 (__  ) / / / / /_/ / / / /
/____/_/_/ /_/\____/_/ /_/ 
""" + Fore.YELLOW + """     %s Version: %s\n""" % (component_name, version)
    logo += Fore.RESET
    print logo
