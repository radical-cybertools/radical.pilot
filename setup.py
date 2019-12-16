#!/usr/bin/env python

__author__    = 'RADICAL Team'
__email__     = 'radical@rutgers.edu'
__copyright__ = 'Copyright 2013-19, RADICAL Research, Rutgers University'
__license__   = 'MIT'


''' Setup script, only usable via pip. '''

import re
import os
import sys
import glob
import shutil

import subprocess as sp

from distutils.ccompiler import new_compiler
from setuptools          import setup, Command, find_packages


# ------------------------------------------------------------------------------
name     = 'radical.pilot'
mod_root = 'src/radical/pilot/'


# ------------------------------------------------------------------------------
#
def sh_callout(cmd):

    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)

    stdout, stderr = p.communicate()
    ret            = p.returncode
    return stdout, stderr, ret


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
def get_version(mod_root):
    '''
    mod_root
        a VERSION file containes the version strings is created in mod_root,
        during installation.  That file is used at runtime to get the version
        information.
        '''

    try:

        version_base   = None
        version_detail = None

        # get version from './VERSION'
        src_root = os.path.dirname(__file__)
        if  not src_root:
            src_root = '.'

        with open(src_root + '/VERSION', 'r') as f:
            version_base = f.readline().strip()

        # attempt to get version detail information from git
        # We only do that though if we are in a repo root dir,
        # ie. if 'git rev-parse --show-prefix' returns an empty string --
        # otherwise we get confused if the ve lives beneath another repository,
        # and the pip version used uses an install tmp dir in the ve space
        # instead of /tmp (which seems to happen with some pip/setuptools
        # versions).
        out, err, ret = sh_callout(
            'cd %s ; '
            'test -z `git rev-parse --show-prefix` || exit -1; '
            'tag=`git describe --tags --always` 2>/dev/null ; '
            'branch=`git branch | grep -e "^*" | cut -f 2- -d " "` 2>/dev/null ; '
            'echo $tag@$branch' % src_root)
        version_detail = out.strip()
        version_detail = version_detail.decode()
        version_detail = version_detail.replace('detached from ', 'detached-')

        # remove all non-alphanumeric (and then some) chars
        version_detail = re.sub('[/ ]+', '-', version_detail)
        version_detail = re.sub('[^a-zA-Z0-9_+@.-]+', '', version_detail)

        if  ret            !=  0  or \
            version_detail == '@' or \
            'git-error'      in version_detail or \
            'not-a-git-repo' in version_detail or \
            'not-found'      in version_detail or \
            'fatal'          in version_detail :
            version = version_base
        elif '@' not in version_base:
            version = '%s-%s' % (version_base, version_detail)
        else:
            version = version_base

        # make sure the version files exist for the runtime version inspection
        path = '%s/%s' % (src_root, mod_root)
        with open(path + '/VERSION', 'w') as f:
            f.write(version + '\n')

        sdist_name = '%s-%s.tar.gz' % (name, version)
        sdist_name = sdist_name.replace('/', '-')
        sdist_name = sdist_name.replace('@', '-')
        sdist_name = sdist_name.replace('#', '-')
        sdist_name = sdist_name.replace('_', '-')

        if '--record'    in sys.argv or \
           'bdist_egg'   in sys.argv or \
           'bdist_wheel' in sys.argv    :
            # pip install stage 2 or easy_install stage 1
            #
            # pip install will untar the sdist in a tmp tree.  In that tmp
            # tree, we won't be able to derive git version tags -- so we pack
            # the formerly derived version as ./VERSION
            shutil.move("VERSION", "VERSION.bak")            # backup version
            shutil.copy("%s/VERSION" % path, "VERSION")      # use full version
            os.system  ("python setup.py sdist")             # build sdist
            shutil.copy('dist/%s' % sdist_name,
                        '%s/%s'   % (mod_root, sdist_name))  # copy into tree
            shutil.move('VERSION.bak', 'VERSION')            # restore version

        with open(path + '/SDIST', 'w') as f:
            f.write(sdist_name + '\n')

        return version_base, version_detail, sdist_name

    except Exception as e:
        raise RuntimeError('Could not extract/set version: %s' % e)


# ------------------------------------------------------------------------------
# check python version. we need >= 2.7, <3.x
if  sys.hexversion <= 0x03050000:
    raise RuntimeError('%s requires Python 3.5 or higher' % name)


# ------------------------------------------------------------------------------
# get version info -- this will create VERSION and srcroot/VERSION
version, version_detail, sdist_name = get_version(mod_root)


# ------------------------------------------------------------------------------
#
def read(*rnames):

    try:
        return open(os.path.join(os.path.dirname(__file__), *rnames)).read()
    except Exception:
        return ''


# ------------------------------------------------------------------------------
#
class RunTwine(Command):
    user_options = []
    def initialize_options (self) : pass
    def finalize_options   (self) : pass
    def run (self) :
        out,  err, ret = sh_callout('python setup.py sdist upload -r pypi')
        raise SystemExit(ret)


# ------------------------------------------------------------------------------
#
# This copies the contents like examples/ dir under sys.prefix/share/$name
# It needs the MANIFEST.in entries to work.
base = 'share/%s' % name
df = [('%s/'                      % base, ['docs/source/events.md']),
      ('%s/examples'              % base, glob.glob('examples/[01]*.py')),
      ('%s/examples'              % base, glob.glob('examples/hello*')),
      ('%s/examples'              % base, glob.glob('examples/*.json')),
      ('%s/examples/docs'         % base, glob.glob('examples/docs/*')),
      ('%s/examples/misc'         % base, glob.glob('examples/misc/*')),
      ('%s/examples/kmeans'       % base, glob.glob('examples/kmeans/*')),
      ('%s/examples/mandelbrot'   % base, glob.glob('examples/mandelbrot/*')),
      ('%s/examples/data_staging' % base, glob.glob('examples/data_staging/*')),
]


# ------------------------------------------------------------------------------
#
setup_args = {
    'name'               : name,
    'namespace_packages' : ['radical'],
    'version'            : version,
    'description'        : 'The RADICAL pilot job framework',
  # 'long_description'   : (read('README.md') + '\n\n' + read('CHANGES.md')),
    'author'             : 'RADICAL Group at Rutgers University',
    'author_email'       : 'radical@rutgers.edu',
    'maintainer'         : 'The RADICAL Group',
    'maintainer_email'   : 'radical@rutgers.edu',
    'url'                : 'https://www.github.com/radical-cybertools/radical.pilot/',
    'license'            : 'MIT',
    'keywords'           : 'radical pilot job saga',
    'python_requires'    : '>=3.5',
    'classifiers'        : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],
    'packages'           : find_packages('src'),
    'package_dir'        : {'': 'src'},
    'scripts'            : [
                            'bin/radical-pilot-bridge',
                            'bin/radical-pilot-bson2json',
                            'bin/radical-pilot-cleanup',
                            'bin/radical-pilot-close-session',
                            'bin/radical-pilot-component',
                            'bin/radical-pilot-create-static-ve',
                            'bin/radical-pilot-deploy-ompi.sh',
                            'bin/radical-pilot-fetch-db',
                            'bin/radical-pilot-fetch-logfiles',
                            'bin/radical-pilot-fetch-profiles',
                            'bin/radical-pilot-fetch-logfiles',
                            'bin/radical-pilot-fetch-json',
                            'bin/radical-pilot-inspect',
                            'bin/radical-pilot-prte2prof',
                            'bin/radical-pilot-run-session',
                            'bin/radical-pilot-stats',
                            'bin/radical-pilot-stats.plot',
                            'bin/radical-pilot-version',
                            'bin/radical-pilot-agent',
                            'bin/radical-pilot-agent-funcs',
                            'bin/radical-pilot-agent-statepush'
                           ],
    'package_data'       : {'': ['*.txt', '*.sh', '*.json', '*.gz', '*.c',
                                 '*.md', 'VERSION', 'SDIST', sdist_name]},
  # 'setup_requires'     : ['pytest-runner'],
    'install_requires'   : ['radical.utils>=0.90',
                            'radical.saga>=0.90',
                            'pymongo',
                            'python-hostlist',
                            'netifaces',
                            'setproctitle',
                            'ntplib',
                            'msgpack-python'
                           ],
    'extras_require'     : {'autopilot' : ['github3.py']},
    'tests_require'      : ['pytest',
                            'pylint',
                            'flake8',
                            'coverage',
                            'mock==2.0.0.',
                           ],
    'test_suite'         : '%s.tests' % name,
    'zip_safe'           : False,
  # 'build_sphinx'       : {
  #     'source-dir'     : 'docs/',
  #     'build-dir'      : 'docs/build',
  #     'all_files'      : 1,
  # },
  # 'upload_sphinx'      : {
  #     'upload-dir'     : 'docs/build/html',
  # },
    # This copies the contents of the examples/ dir under
    # sys.prefix/share/$name
    # It needs the MANIFEST.in entries to work.
    'data_files'         : df,
    'cmdclass'           : {'upload': RunTwine},
}


# ------------------------------------------------------------------------------
#
setup(**setup_args)

os.system('rm -rf src/%s.egg-info' % name)


# ------------------------------------------------------------------------------


