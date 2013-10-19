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

            print "XXXXXXXX: %s" % out

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


#-----------------------------------------------------------------------------
#
def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()

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
                        'radical.utils'],
      test_suite = 'sinon.tests',
      package_data = {'': ['*.sh']},
      include_package_data = True,
      zip_safe = False,
      cmdclass = {
          'install_data': sinon_install_data,
          'sdist': sinon_sdist
      },
)
