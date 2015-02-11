__author__    = "RADICAL Team"
__copyright__ = "Copyright 2013, RADICAL Research, Rutgers University"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess as sp

from setuptools import setup, Command, find_packages

name     = 'radical.pilot'
mod_root = 'src/radical/pilot/'

# ------------------------------------------------------------------------------
#
# versioning mechanism:
#
#   - version:          1.2.3            - is used for installation
#   - version_detail:  v1.2.3-9-g0684b06 - is used for debugging
#   - version is read from VERSION file in src_root, which then is copied to
#     module dir, and is getting installed from there.
#   - version_detail is derived from the git tag, and only available when
#     installed from git.  That is stored in mod_root/VERSION in the install
#     tree.
#   - The VERSION file is used to provide the runtime version information.
#
def get_version (mod_root):
    """
    mod_root
        a VERSION file containes the version strings is created in mod_root,
        during installation.  That file is used at runtime to get the version
        information.  
        """

    try:

        version        = None
        version_detail = None

        # get version from './VERSION'
        src_root = os.path.dirname (__file__)
        if  not src_root :
            src_root = '.'

        with open (src_root + '/VERSION', 'r') as f :
            version = f.readline ().strip()


        # attempt to get version detail information from git
        p   = sp.Popen ('cd %s ; '\
                        'tag=`git describe --tags --always` 2>/dev/null ; '\
                        'branch=`git branch | grep -e "^*" | cut -f 2 -d " "` 2>/dev/null ; '\
                        'echo $tag@$branch'  % src_root,
                        stdout=sp.PIPE, stderr=sp.STDOUT, shell=True)
        version_detail = p.communicate()[0].strip()

        if  p.returncode   !=  0  or \
            version_detail == '@' or \
            'fatal'        in version_detail :
            version_detail =  'v%s' % version

        print 'version: %s (%s)'  % (version, version_detail)


        # make sure the version files exist for the runtime version inspection
        path = '%s/%s' % (src_root, mod_root)
        print 'creating %s/VERSION' % path

        sdist_name = "radical.pilot-%s.tar.gz" % version
        if '--record'  in sys.argv or 'bdist_egg' in sys.argv :   
           # pip install stage 2      easy_install stage 1
            os.system ("python setup.py sdist")
            os.system ("cp 'dist/%s' 'src/radical/pilot/%s'" % (sdist_name, sdist_name))

        with open (path + "/SDIST",       "w") as f : f.write (sdist_name     + "\n")
        with open (path + "/VERSION",     "w") as f : f.write (version_detail + "\n")

        return version, version_detail, sdist_name

    except Exception as e :
        raise RuntimeError ('Could not extract/set version: %s' % e)


# ------------------------------------------------------------------------------
# get version info -- this will create VERSION and srcroot/VERSION
version, version_detail, sdist_name = get_version (mod_root)


# ------------------------------------------------------------------------------
# check python version. we need >= 2.7, <3.x
if  sys.hexversion < 0x02070000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("%s requires Python 2.x (2.7 or higher)" % name)


# ------------------------------------------------------------------------------
#
def read(*rnames):
    try :
        return open(os.path.join(os.path.dirname(__file__), *rnames)).read()
    except Exception :
        return ''


# -------------------------------------------------------------------------------
setup_args = {
    'name'             : name,
    'version'          : version,
    'description'      : "The RADICAL pilot job framework",
    'long_description' : (read('README.md') + '\n\n' + read('CHANGES.md')),
    'author'           : 'RADICAL Group at Rutgers University',
    'author_email'     : "radical@rutgers.edu",
    'maintainer'       : "Ole Weidner",
    'maintainer_email' : "ole.weidner@rutgers.edu",
    'url'              : "https://github.com/radical-cybertools/radical.pilot",
    'license'          : "MIT",
    'keywords'         : "radical pilot job saga",
    'classifiers'      : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],
    'namespace_packages': ['radical'],
    'packages'           : find_packages('src'),
    'package_dir'        : {'': 'src'},
    'scripts'            : ['bin/radicalpilot-bson2json',
                            'bin/radicalpilot-inspect',
                            'bin/radicalpilot-version',
                            'bin/radicalpilot-close-session',
                            'bin/radicalpilot-cleanup',
                            'bin/radicalpilot-stats',
                            'bin/radicalpilot-stats.plot',
                            'src/radical/pilot/agent/radical-pilot-agent-multicore.py'
                           ],
    'package_data'       : {'': ['*.sh', '*.json', 'VERSION', 'SDIST', sdist_name]},

    'install_requires'   : ['saga-python',
                            'radical.utils',
                            'pymongo>=2.5',
                            'python-hostlist'],
    'tests_require'      : ['setuptools', 'nose'],
    'test_suite'         : 'radical.pilot.tests',
    'zip_safe'           : False,
}

# ------------------------------------------------------------------------------

setup (**setup_args)

# ------------------------------------------------------------------------------

