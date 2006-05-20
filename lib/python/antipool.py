#!/usr/bin/env python
# Copyright (C) 2006 Martin Blais. All Rights Reserved.

"""
An implementation of a DBAPI-2.0 connection pooling system in a multi-threaded
environment.

Initialization
--------------

To use connection pooling, you must first create a connection pool object::

    database = ConnectionPool(dbapi,
                              database='test',
                              user='blais')

where 'dbapi' is the module that you want to use that implements the DBAPI-2.0
interface.  You need only create a single instance of this object for your
process, and you could make database globally accessible.

Configuration
-------------

The connection pool has a few configuration options.  See the constructor's
'options' parameter for details.

Acquiring Connections
---------------------

Then, when you want to get a connection to perform some operations on the
database, you call the connection() method and use it the usual DBAPI way::

    conn = database.connection()
    cursor = conn.cursor()
    ...
    conn.commit()

Read-Only Connections
---------------------

If the connection objects can be shared between threads, the connection pool
allows you to perform an optimization which consists in sharing the connection
between all the threads, for read-only operations.  When you know that you will
not need to modify the database for a transaction, get your connection using the
connection_ro() method::

    conn = database.connection_ro()
    cursor = conn.cursor()
    ...

Since this will not work for operations that write to the database, you should
NEVER perform inserts, deletes or updates using these special connections.  We
do not check the SQL that gets executed, but we specifically do not provide a
commit() method on the connection wrapper so that your code blows up if you try
to commit, which will help you find bugs if you make mistakes with this.

Releasing Connections
---------------------

The connection objects that are provided by the pool are created on demand, and
a goal of the pool is to minimize the amount of resources needed by your
application.  The connection objects will normally automatically be released to
the pool once they get collected by your Python interpreter.  However, the
Python implementation that you are using may be keeping the connection objects
alive for some time you have finished using them.  Therefore, in order to
minimize the number of live connections at any time, you should always release
the connection objects with the release() method after you have finished using
them::

    conn = database.connection()
    ...
    ...
    conn.release()

We recommend using a try-finally form to make it exception-safe::

    conn = database.connection()
    try:
        cursor = conn.cursor()
        ...
    finally:
        conn.release()

Note that if you forget to release the connections it does not create a leak, it
only causes a slightly less efficient use of the connection resources.  No big
deal.

Finalization
------------

On application exit, you should finalize the connection pool explicitly, to
close the database connections still present in the pool::

    database.finalize()

It will finalize itself automatically if you forget, but in the interpreter's
finalization stage, which happens in a partially destroyed environment.  It is
always safer to finalize explictly.

Testing
-------

To run a multi-threaded simnulation program using this module, just run it
directly.  The --debug option provides more verbose output of the connection
pool behaviour.

Supported Databases
-------------------

Currently, we have tested this module with the following databases:

* PostgreSQL (8.x)


"""

__author__ = 'Martin Blais <blais@furius.ca>'
__copyright__ = 'Copyright (C) 2006 Martin Blais. All Rights Reserved.'


# stdlib imports
import sys, thread, threading, gc, warnings
from datetime import datetime, timedelta


__all__ = ('ConnectionPool', 'Error')


class ConnectionPool(object):
    """
    A pool of database connections that can be shared by a number of threads.
    """

    _minconn = 5
    """The minimum number of connections to keep around."""

    _minkeepsecs = 5 # seconds
    """The minimum amount of seconds that we should keep connections around
    for."""

    def __init__( self, dbapi, options=None, **params ):
        """
        'dbapi': the DBAPI-2.0 module interface for creating connections.
        'minconn': the minimum number of connections to keep around.
        'debug': flag to enable printing debugging output.
        '**params': connection parameters for creating a new connection.
        """
        self.dbapi = dbapi

        if options is None:
            options = {}
        
        minconn = options.pop('minconn', None)
        if minconn is not None:
            self._minconn = minconn

        minkeepsecs = options.pop('minkeepsecs', None)
        if minkeepsecs is not None:
            self._minkeepsecs = minkeepsecs

        self._params = params
        """The parameters for creating a connection."""

        self._pool = []
        self._pool_lock = threading.RLock()
        """A pool of database connections and an associated lock for access."""

        self._nbcreated = 0
        """The number read-write database connections that were handed out."""

        self._roconn = None
        self._roconn_lock = threading.Lock()
        self._roconn_refs = 0
        """A connection for read-only access and an associated lock for
        creation.  We also store the number of references to it that were
        handled to clients."""

        self._debug = options.pop('debug', False)
        
        disable_ro = options.pop('disable_ro', False)
        if disable_ro or dbapi.threadsafety < 2:
            # Disable the RO connections.
            self.connection_ro = self.connection_ro_loser

            if 1 or not disable_ro and dbapi.threadsafety < 2:
                # Note: Configure with disable_ro to remove this warning
                # message.
                warnings.warn(
                    "Warning: Your DBAPI module '%s' does not support sharing "
                    "connections between threads." % str(dbapi))
                
    def module( self ):
        """
        Get access to the DBAPI-2.0 module.  This is necessary for some of the
        standard objects it provides, e.g. Binary().
        """
        return self.dbapi

    def _log( self, msg ):
        """
        Debugging information logging.
        """
        if self._debug:
            sys.stderr.write('   [%06d] %s\n' % (thread.get_ident(), msg))
        
    def _connect( self ):
        """
        Create a new connection to the database.
        """
        self._log('Connection Create')
        return apply(self.dbapi.connect, (), self._params)

    def _close( self, conn ):
        """
        Create a new connection to the database.
        """
        self._log('Connection Close')
        return conn.close()

    def connection_ro( self ):
        """
        Acquire a connection for read-only operations.
        """
        self._roconn_lock.acquire()
        self._log('Acquire RO')
        try:
            if not self._roconn:
                self._roconn = self._connect()
            self._roconn_refs += 1
        finally:
            self._roconn_lock.release()

        return ConnectionWrapperRO(self._roconn, self)
    
    def _acquire( self ):
        """
        Acquire a connection from the pool, for read an write operations.
        """
        self._pool_lock.acquire()
        self._log('Acquire  Pool: %d  / Created: %s' %
                    (len(self._pool), self._nbcreated))
        try:
            if self._pool:
                conn, last_released = self._pool.pop()
            else:
                conn = self._connect()
                self._nbcreated += 1
        finally:
            self._pool_lock.release()
        return conn

    def connection_ro_loser( self ):
        """
        Version of the connection_ro() that actually uses the pool to get its
        connections.  This is used when the dbapi does not allow threads to
        share a connection.
        """
        conn = self._acquire()
        return ConnectionWrapperSemi(conn, self)

    def connection( self ):
        """
        Acquire a connection for read an write operations.
        """
        conn = self._acquire()
        return ConnectionWrapper(conn, self)


    def _release_ro( self, conn ):
        """
        Release a reference to the read-only connection.  You should not use
        this directly, you should instead call release() or close() on the
        connection object.
        """
        self._roconn_lock.acquire()
        assert self._roconn
        assert conn is self._roconn
        
        self._roconn_refs -= 1
        self._log('Release RO')
        self._roconn_lock.release()

    def _release( self, conn ):
        """
        Release a reference to a read-and-write connection.
        """
        self._pool_lock.acquire()
        assert conn is not self._roconn
        self._log('Release  Pool: %d  / Created: %s' %
                  (len(self._pool), self._nbcreated))
        self._pool.append( (conn, datetime.now()) )
        self._scaledown()
        self._pool_lock.release()

    def _scaledown( self ):
        """
        Scale down the number of connection according to the following
        heuristic: we want keep a minimum number of extra connections in the
        pool ready for usage.  We delete all connections above that number if
        they have last been used beyond a fixed timeout.
        """
        self._pool_lock.acquire()

        # Calculate a recent time limit beyond which we always keep the
        # connections.
        minkeepsecs = datetime.now() - timedelta(seconds=self._minkeepsecs)

        # Calculate the number of connections that we can get rid of.
        n = len(self._pool) - self._minconn
        if n > 0:
            filtered_pool = []
            for poolitem in self._pool:
                conn, last_released = poolitem
                if n > 0 and last_released < minkeepsecs:
                    self._close(conn)
                    self._nbcreated -= 1
                    n -= 1
                else:
                    filtered_pool.append(poolitem)
            self._pool = filtered_pool
        self._pool_lock.release()

        # Note: we could keep the pool sorted by last_released to minimize the
        # scaledown time, so that the first items in the pool are always the
        # oldest, the most likely to be deleteable.


    def finalize( self ):
        """
        Close all the open connections and finalize.
        """
        if not self._pool and not self._roconn:
            assert self._nbcreated == 0
            return # Already finalized.
        
        # Make sure that all connections lying about are collected before we go
        # on.
        gc.collect()

        # Check that all the connections have been returned to us.
        assert len(self._pool) == self._nbcreated

        assert self._roconn_refs == 0
        if self._roconn is not None:
            self._close(self._roconn)
            self._roconn = None

        # Release all the read-write pool's connections.
        for conn, last_released in self._pool:
            self._close(conn)

        poolsize = len(self._pool)
        self._pool = []

        self._log('Finalize  Pool: %d  / Created: %s' %
                  (poolsize, self._nbcreated))

        # Reset statistics.
        self._nbcreated = 0


    def __del__( self ):
        """
        Destructor.
        """
        self.finalize()

class ConnectionWrapperRO(object):
    """
    A wrapper object that behaves like a database connection for read-only
    operations.  You cannot close() this explicitly, you should call release().

    Important: you should always try to explicitly release these objects, in
    order to minimize the number of open connections in the pool.  If you do not
    release explicitly, the pool has to keep the connection opne.  Here is the
    preferred way to do this:

       connection = database.connection()
       try:
           # you code here
       finally:
           connection.release()

    Note that this connection wrapper does not allow committing.  It is meant
    for read-only operations (i.e. SELECT). See class ConnectionWrapper for the
    commit method.
    """
    def __init__( self, conn, pool ):
        assert conn
        self._conn = conn
        self._connpool = pool

    def __del__( self ):
        if self._conn:
            self.release()

    def _getconn( self ):
        if self._conn is None:
            raise Error("Error: Connection already closed.")
        else:
            return self._conn

    def release( self ):
        self._release_impl(self._getconn())
        self._connpool = self._conn = None

    def _release_impl( self, conn ):
        self._connpool._release_ro(conn)

    def cursor( self ):
        return self._getconn().cursor()

    def commit( self ):
        raise Error("Error: You cannot commit on a read-only connection.")

    def rollback( self ):
        return self._getconn().rollback()


class ConnectionWrapperSemi(ConnectionWrapperRO):
    """
    A wrapper object that releases to the pool.  It still does not provide a
    commit() method however.
    """
    def _release_impl( self, conn ):
        self._connpool._release(conn)

class ConnectionWrapper(ConnectionWrapperSemi):
    """
    A wrapper object that allows write operations and provides a commit()
    method.  See ConnectionWrapperRO for more details.
    """
    def commit( self ):
        return self._getconn().commit()

class Error(Exception):
    """
    Error for connection wrappers.
    """



#===============================================================================
# TESTS
#===============================================================================

import random, time

names = ('martin', 'cyriaque', 'pierre', 'mathieu', 'marie-claude', 'eric'
         'normand', 'christine', 'emric')

class TestThreads(threading.Thread):

    def __init__( self, opts ):
        threading.Thread.__init__(self)

        self.opts = opts
        self._stop = False

    def stop( self ):
        self._stop = True

    def run( self ):
        timeout = (datetime.now() + timedelta(seconds=self.opts.timeout))

        while not self._stop and datetime.now() < timeout:
            time.sleep(random.uniform(0, self.opts.time_wait))

            try:
                if random.random() < self.opts.prob_ro:
                    # Read-only operation.
                    conn = database.connection_ro()

                    curs = conn.cursor()
                    curs.execute("""
                      SELECT name FROM things LIMIT %s;
                      """ % random.randint(0, 5))
                    print 'SELECT',
                    for row in curs:
                        print row[0],
                    print

                else:
                    conn = database.connection()

                    curs = conn.cursor()
                    things = (random.choice(names), self.getName())
                    print 'INSERT', things
                    curs.execute("""
                      INSERT INTO things (name, thread) VALUEs (%s, %s);
                      """, things)
                    conn.commit()

            finally:
                time.sleep(self.opts.time_hold)
                if random.random() < self.opts.prob_forget:
                    conn.release()


def test():
    import optparse
    parser = optparse.OptionParser(__doc__.strip())

    parser.add_option('-d', '--debug', action='store_true',
                      help="Enable debugging output.")

    parser.add_option('-t', '--threads', action='store', type='int',
                      default=10,
                      help="Number of threads to create.")

    parser.add_option('--prob-ro', action='store', type='float',
                      default=0.8,
                      help="Specify the read-only to read-and-write ratio "
                      "as a PDF.")

    parser.add_option('--prob-forget', action='store', type='float',
                      default=0.1,
                      help="Probability to forget to release the connection.")

    parser.add_option('--timeout', action='store', type='float',
                      default=10,
                      help="Total time for the experiment")

    parser.add_option('--time-wait', action='store', type='float',
                      default=2.0, metavar='SECS',
                      help="Maximum time to wait between each operations.")

    parser.add_option('--time-hold', action='store', type='float',
                      default=0.1, metavar='SECS',
                      help="Time to hold a connection for an operation.")

    parser.add_option('-c', '--minconn', action='store', type='int',
                      default=5,
                      help="Minimum number of connections to keep around when "
                      "scaling down the pool.")

    parser.add_option('-k', '--minkeepsecs', action='store', type='float',
                      default=5,
                      help="Default seconds to keep a connection for when "
                      "scaling down the pool.")

    parser.add_option('--disable-ro', action='store_true',
                      help="Disable the read-only optimization.")


    opts, args = parser.parse_args()

    global database
    import psycopg2
    database = ConnectionPool(psycopg2,
                              options=dict(minconn=opts.minconn,
                                           minkeepsecs=opts.minkeepsecs,
                                           disable_ro=opts.disable_ro,
                                           debug=opts.debug),
                              database='test',
                              user='blais')

    # Create some tables.
    conn = database.connection()
    try:
        curs = conn.cursor()
        try:
            curs.execute(test_drop)
        except psycopg2.Error:
            conn.rollback()
        curs.execute(test_schema)
        conn.commit()
    finally:
        conn.release()

    # Create threads that operate concurrently on that table.
    threads = []
    for i in xrange(opts.threads):
        t = TestThreads(opts)
        threads.append(t)
        t.start()
        
    try:
        time.sleep(opts.timeout)
    except KeyboardInterrupt:
        print 'Interrupted.'
        for t in threads:
            t.stop()

    for t in threads:
        t.join()

    database.finalize()


test_drop = '''

  DROP TABLE things;

'''

test_schema = '''

  CREATE TABLE things (

      id SERIAL PRIMARY KEY,
      name TEXT,
      thread TEXT

  );

'''

if __name__ == '__main__':
    test()

