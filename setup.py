#!/usr/bin/env python

"""
Install script for the Anti-ORM library.
"""

__author__ = "Martin Blais <blais@furius.ca>"

from distutils.core import setup

def read_version():
    try:
        return open('VERSION', 'r').readline().strip()
    except IOError, e:
        raise SystemExit(
            "Error: you must run setup from the root directory (%s)" % str(e))


# Include VERSION without having to create MANIFEST.in
# (I don't like all those files for setup.)
def deco_add_defaults(fun):
    def f(self):
        self.filelist.append('VERSION')
        return fun(self)
    return f
from distutils.command.sdist import sdist
sdist.add_defaults = deco_add_defaults(sdist.add_defaults)


setup(name="antiorm",
      version=read_version(),
      description=\
      "A Pythonic Helper for DBAPI-2.0 SQL Access",
      long_description="""
Anti-ORM is not an ORM, and it certainly does not want to be.  Anti-ORM is a
simple Python module that provides a pythonic syntax for making it more
convenient to build SQL queries over the DBAPI-2.0 interface.

In practice, if you're the kind of person that likes it to the bare metal, it's
almost as good as the ORMs.  At least there is no magic, and it just works.
""",
      license="GPL",
      author="Martin Blais",
      author_email="blais@furius.ca",
      url="http://furius.ca/antiorm",
      package_dir = {'': 'lib/python'},
      py_modules = ('antiorm', 'dbapiext', 'antipool', 'dbrelmgr'),
      ## data_files=[('Poodle', ['VERSION'])],
     )

