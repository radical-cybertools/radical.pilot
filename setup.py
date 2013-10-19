#!/usr/bin/env python
# encoding: utf-8

"""Setup for SAGA-Pilot package
"""

import os, sys
from distutils.command.install_data import install_data
from distutils.command.sdist import sdist
from setuptools import setup, find_packages

#-----------------------------------------------------------------------------
# figure out the current version
def update_version():
    """
    Updates the version based on git tags
    """

    version = 'latest'

    try:
        cwd = os.path.dirname(os.path.abspath(__file__))
        fn = os.path.join(cwd, 'src/sinon/VERSION')
        version = open(fn).read().strip()
    except IOError:
        from subprocess import Popen, PIPE, STDOUT
        import re

        VERSION_MATCH = re.compile(r'\d+\.\d+\.\d+(\w|-)*')

        try:
            p = Popen(['git', 'describe', '--tags', '--always'],
                      stdout=PIPE, stderr=STDOUT)
            out = p.communicate()[0]

            if (not p.returncode) and out:
                v = VERSION_MATCH.search(out)
                if v:
                    version = v.group()
        except OSError:
            pass

    return version

#-----------------------------------------------------------------------------
# check python version. we need > 2.5
if sys.hexversion < 0x02050000:
    raise RuntimeError("Sinon requires Python 2.5 or better")


#-----------------------------------------------------------------------------
# 
class sinon_install_data(install_data):

    def finalize_options(self): 
        self.set_undefined_options('install',
                                   ('install_lib', 'install_dir'))
        install_data.finalize_options(self)

    def run(self):
        install_data.run(self)
        # ensure there's a radical/utils/VERSION file
        fn = os.path.join(self.install_dir, 'src/sinon', 'VERSION')

        if os.path.exists(fn):
            os.remove(fn)

        open(fn, 'w').write(update_version())
        self.outfiles.append(fn)


#-----------------------------------------------------------------------------
# 
class sinon_sdist(sdist):

    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)

        fn = os.path.join(base_dir, 'radical/utils', 'VERSION')

        if os.path.exists(fn):
            os.remove(fn)

        open(fn, 'w').write(update_version())


    def run(self):
        import sys
        import subprocess
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        errno = subprocess.call([sys.executable, '%s/run_tests.py' % testdir,
                                '--config=%s/configs/basetests.cfg' % testdir])
        raise SystemExit(errno)


setup_args = {
    'name': "sinon",
    'version': update_version(),
    'description': "SAGA based Pilot Framework",
    'long_description': "SAGA based Pilot Framework",
    'author': "RADICAL@Rutgers", 
    'author_email': "radical@rutgers.edu",
    'maintainer': "RADICAL",
    'maintainer_email': "radical@rutgers.edu",
    'url': "http://saga-project.github.com/sinon/",
    'license': "LGPL3",
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'License :: OSI Approved :: LGPL3 License',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: AIX',
        'Operating System :: POSIX :: BSD',
        'Operating System :: POSIX :: BSD :: BSD/OS',
        'Operating System :: POSIX :: BSD :: FreeBSD',
        'Operating System :: POSIX :: BSD :: NetBSD',
        'Operating System :: POSIX :: BSD :: OpenBSD',
        'Operating System :: POSIX :: GNU Hurd',
        'Operating System :: POSIX :: HP-UX',
        'Operating System :: POSIX :: IRIX',
        'Operating System :: POSIX :: Linux',
        'Operating System :: POSIX :: Other',
        'Operating System :: POSIX :: SCO',
        'Operating System :: POSIX :: SunOS/Solaris',
        'Operating System :: Unix'
    ],
    'packages': [
        "sinon",
        "sinon.api",
        "sinon.v1",
        "sinon.bj",
        "sinon.bj.bj_dummy",
        "sinon.utils",
    ],
    'package_data': {'': ['*.sh']},
    'zip_safe': False,
    'scripts': [],
    # mention data_files, even if empty, so install_data is called and
    # VERSION gets copied
    'data_files': [("sinon", [])],
    'cmdclass': {
        'install_data': our_install_data,
        'sdist': our_sdist,
        'test': our_test
    },
    'install_requires': [],
    'tests_require': ['setuptools', 'nose', 'radical.utils']
}

setup(**setup_args)

#-----------------------------------------------------------------------------
#
setup(name='sinon',
      version=update_version(),
      author='RADICAL Group | Rutgers University',
      author_email='ole.weidner@rutgers.edu',
      description="A SAGA-based pilot framework",
      long_description=(read('README.md') + '\n\n' + read('CHANGES.md')),
      license='MIT',
      keywords="radical pilot job saga",
      classifiers = [
          'Development Status :: 5 - Production/Stable',
          'Environment :: No Input/Output (Daemon)',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Topic :: Internet :: WWW/HTTP',
          'Framework :: Rhythmos'],
      url='https://github.com/saga-project/sinon',
      packages=find_packages('src'),
      package_dir = {'': 'src'},
      #namespace_packages=['sinon'],
      scripts=['bin/sinon-version'],
      install_requires=['setuptools',
                        'saga-python',
                        'radical.utils',
                        'pymongo'],
      test_suite = 'sinon.tests',
      package_data = {'': ['*.sh', 'src/sinon/VERSION']},
      include_package_data = True,
      zip_safe = False,
      cmdclass = {
          'install_data': sinon_install_data,
          'sdist': sinon_sdist
      },
)
