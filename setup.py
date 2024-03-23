#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__email__     = 'info@radical-cybertools.org'
__copyright__ = 'Copyright 2013-23, The RADICAL-Cybertools Team'
__license__   = 'MIT'


''' Setup script, only usable via pip. '''

import re
import os
import sys
import glob
import shutil

import subprocess as sp

from setuptools import setup, Command, find_namespace_packages
from pathlib    import Path


# ------------------------------------------------------------------------------
base     = 'pilot'
name     = 'radical.%s'      % base
mod_root = 'src/radical/%s/' % base

# ------------------------------------------------------------------------------
#
# pip warning:
# "In-tree builds are now default. pip 22.1 will enforce this behaviour change.
#  A possible replacement is to remove the --use-feature=in-tree-build flag."
#
# With this change we need to make sure to clean out all temporary files from
# the src tree. Specifically create (and thus need to clean)
#   - VERSION
#   - SDIST
#   - the sdist file itself (a tarball)
#
# `pip install` (or any other direct or indirect invocation of `setup.py`) will
# in fact run `setup.py` multiple times: one on the top level, and internally
# again with other arguments to build sdist and bwheel packages.  We must *not*
# clean out temporary files in those internal runs as that would invalidate the
# install.
#
# We thus introduce an env variable `SDIST_LEVEL` which allows us to separate
# internal calls from the top level invocation - we only clean on the latter
# (see end of this file).
sdist_level = int(os.environ.get('SDIST_LEVEL', 0))
os.environ['SDIST_LEVEL'] = str(sdist_level + 1)

root = os.path.dirname(__file__) or '.'


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
#   - version is read from VERSION file in root, which then is copied to
#     module dir, and is getting installed from there.
#   - version_detail is derived from the git tag, and only available when
#     installed from git.  That is stored in mod_root/VERSION in the install
#     tree.
#   - The VERSION file is used to provide the runtime version information.
#
def get_version(_mod_root):
    '''
    a VERSION file containes the version strings is created in mod_root,
    during installation.  That file is used at runtime to get the version
    information.
    '''

    try:

        _version_base   = None
        _version_detail = None
        _sdist_name     = None

        # get version from './VERSION'
        with open('%s/VERSION' % root, 'r', encoding='utf-8') as fin:
            _version_base = fin.readline().strip()

        # attempt to get version detail information from git
        # We only do that though if we are in a repo root dir,
        # ie. if 'git rev-parse --show-prefix' returns an empty string --
        # otherwise we get confused if the ve lives beneath another repository,
        # and the pip version used uses an install tmp dir in the ve space
        # instead of /tmp (which seems to happen with some pip/setuptools
        # versions).
        out, _, ret = sh_callout(
            'cd %s ; '
            'test -z `git rev-parse --show-prefix` || exit -1; '
            'tag=`git describe --tags --always` 2>/dev/null ; '
            'branch=`git branch | grep -e "^*" | cut -f 2- -d " "` 2>/dev/null ; '
            'echo $tag@$branch' % root)
        _version_detail = out.strip()
        _version_detail = _version_detail.decode()
        _version_detail = _version_detail.replace('detached from ', 'detached-')

        # remove all non-alphanumeric (and then some) chars
        _version_detail = re.sub('[/ ]+', '-', _version_detail)
        _version_detail = re.sub('[^a-zA-Z0-9_+@.-]+', '', _version_detail)

        if ret              !=  0  or \
            _version_detail == '@' or \
            'git-error'      in _version_detail or \
            'not-a-git-repo' in _version_detail or \
            'not-found'      in _version_detail or \
            'fatal'          in _version_detail :
            _version = _version_base
        elif '@' not in _version_base:
            _version = '%s-%s' % (_version_base, _version_detail)
        else:
            _version = _version_base

        # make sure the version files exist for the runtime version inspection
        _path = '%s/%s' % (root, _mod_root)
        with open(_path + '/VERSION', 'w', encoding='utf-8') as fout:
            fout.write(_version_base + '\n')
            fout.write(_version      + '\n')

        _sdist_name = '%s-%s.tar.gz' % (name, _version_base)
      # _sdist_name = _sdist_name.replace('/', '-')
      # _sdist_name = _sdist_name.replace('@', '-')
      # _sdist_name = _sdist_name.replace('#', '-')
      # _sdist_name = _sdist_name.replace('_', '-')

        if '--record'    in sys.argv or \
           'bdist_egg'   in sys.argv or \
           'bdist_wheel' in sys.argv    :
            # pip install stage 2 or easy_install stage 1
            #
            # pip install will untar the sdist in a tmp tree.  In that tmp
            # tree, we won't be able to derive git version tags -- so we pack
            # the formerly derived version as ./VERSION
            shutil.move('VERSION', 'VERSION.bak')              # backup
            shutil.copy('%s/VERSION' % _path, 'VERSION')       # version to use
            os.system  ('python3 setup.py sdist')              # build sdist
            shutil.copy('dist/%s' % _sdist_name,
                        '%s/%s'   % (_mod_root, _sdist_name))  # copy into tree
            shutil.move('VERSION.bak', 'VERSION')              # restore version

        with open(_path + '/SDIST', 'w', encoding='utf-8') as fout:
            fout.write(_sdist_name + '\n')

        return _version_base, _version_detail, _sdist_name, _path

    except Exception as e:
        raise RuntimeError('Could not extract/set version: %s' % e) from e


# ------------------------------------------------------------------------------
# get version info -- this will create VERSION and srcroot/VERSION
version, version_detail, sdist_name, path = get_version(mod_root)


# ------------------------------------------------------------------------------
# check python version, should be >= 3.7
if sys.hexversion < 0x03070000:
    raise RuntimeError('ERROR: %s requires Python 3.7 or newer' % name)


# ------------------------------------------------------------------------------
#
class RunTwine(Command):
    user_options = []
    def initialize_options(self): pass
    def finalize_options(self):   pass
    def run(self):
        _, _, ret = sh_callout('python3 setup.py sdist upload -r pypi')
        raise SystemExit(ret)


# ------------------------------------------------------------------------------
#
# This copies the contents like examples/ dir under sys.prefix/share/$name
# It needs the MANIFEST.in entries to work.
base = 'share/%s' % name
df = [('%s/examples'              % base, glob.glob('examples/[01]*.py')),
      ('%s/examples'              % base, glob.glob('examples/hello*')),
      ('%s/examples'              % base, glob.glob('examples/*.json')),
      ('%s/examples/docs'         % base, glob.glob('examples/docs/*.py')),
      ('%s/examples/misc'         % base, glob.glob('examples/misc/*.py')),
      ('%s/examples/misc'         % base, glob.glob('examples/misc/*.cfg')),
      ('%s/examples/data_staging' % base, glob.glob('examples/data_staging/*.py')),
]


# ------------------------------------------------------------------------------
#
with open('%s/requirements.txt' % root, encoding='utf-8') as freq:
    requirements = freq.readlines()


# ------------------------------------------------------------------------------
#
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')


# ------------------------------------------------------------------------------
#
setup_args = {
    'name'               : name,
    'namespace_packages' : ['radical'],
    'version'            : version,
    'description'        : 'The RADICAL pilot job framework',
    'long_description'   : long_description,
    'long_description_content_type' : 'text/markdown',
    'author'             : 'RADICAL Group at Rutgers University',
    'author_email'       : 'radical@rutgers.edu',
    'maintainer'         : 'The RADICAL Group',
    'maintainer_email'   : 'radical@rutgers.edu',
    'url'                : 'http://radical-cybertools.github.io/%s/' % name,
    'project_urls'       : {
        'Documentation': 'https://radical%s.readthedocs.io/en/latest/' % base,
        'Source'       : 'https://github.com/radical-cybertools/%s/'   % name,
        'Issues' : 'https://github.com/radical-cybertools/%s/issues'   % name,
    },
    'license'            : 'MIT',
    'keywords'           : 'radical pilot job',
    'python_requires'    : '>=3.7',
    'classifiers'        : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],
    'packages'           : find_namespace_packages('src', include=['radical.*']),
    'package_dir'        : {'': 'src'},
    'scripts'            : [
                            'bin/radical-pilot-agent_0',
                            'bin/radical-pilot-agent_n',
                            'bin/radical-pilot-agent-statepush',
                            'bin/radical-pilot-bridge',
                            'bin/radical-pilot-component',
                            'bin/radical-pilot-create-static-ve',
                            'bin/radical-pilot-deploy-ompi.sh',
                            'bin/radical-pilot-hello.sh',
                            'bin/radical-pilot-limits.py',
                            'bin/radical-pilot-prte2prof',
                            'bin/radical-pilot-raptor-master',
                            'bin/radical-pilot-raptor-worker',
                            'bin/radical-pilot-resources',
                            'bin/radical-pilot-run-session',
                            'bin/radical-pilot-service-signal',
                            'bin/radical-pilot-stats',
                            'bin/radical-pilot-stats.plot',
                            'bin/radical-pilot-ve',
                            'bin/radical-pilot-version',
                           ],
    'package_data'       : {'': ['*.txt', '*.sh', '*.json', '*.gz', '*.c',
                                 '*.md', 'VERSION', 'SDIST', sdist_name]},
    'install_requires'   : requirements,
    'zip_safe'           : False,
    'data_files'         : df,
    'cmdclass'           : {'upload': RunTwine},
}


# ------------------------------------------------------------------------------
#
setup(**setup_args)


# ------------------------------------------------------------------------------
# clean temporary files from source tree
if sdist_level == 0:
    os.system('rm -vrf src/%s.egg-info' % name)
    os.system('rm -vf  %s/%s'           % (path, sdist_name))
    os.system('rm -vf  %s/VERSION'      % path)
    os.system('rm -vf  %s/SDIST'        % path)


# ------------------------------------------------------------------------------

