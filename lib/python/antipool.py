#!/usr/bin/env python
# Copyright (C) 2006 Martin Blais. All Rights Reserved.

"""
An implementation of a DBAPI-2.0 connection pooling system in a multi-threaded
environment.

Initialization
--------------

To use connection pooling, you must first create a connection pool object::

    dbpool = ConnectionPool(dbapi,
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

    conn = dbpool.connection()
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

    conn = dbpool.connection_ro()
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

    conn = dbpool.connection()
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

    dbpool.finalize()

It will finalize itself automatically if you forget, but in the interpreter's
finalization stage, which happens in a partially destroyed environment.  It is
always safer to finalize explicitly.

Testing
-------

To run a multi-threaded simulation program using this module, just run it
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
import types, threading, gc, warnings
from datetime import datetime, timedelta


__all__ = ('ConnectionPool', 'Error')


#-------------------------------------------------------------------------------
#
class ConnectionPoolInterface(object):
    """
    Interface for a connection pool.  This is documentation for the public
    interface that you are supposed to use.
    """
    def module( self ):
        """
        Get access to the DBAPI-2.0 module.  This is necessary for some of the
        standard objects it provides, e.g. Binary().
        """
    
    def connection( self, nbcursors=0, readonly=False ):
        """
        Acquire a connection for read an write operations.

        As a convenience, additionally create a number of cursors and return
        them along with the connection, for example::

           conn, curs1, curs2 = dbpool.connection(2)

        Invoke with readonly=True if you need a read-only connection
        (alternatively, you can use the connection_ro() method below).
        """

    def connection_ro( self, nbcursors=0 ):
        """
        Acquire a connection for read-only operations.
        See connection() for details.
        """

    def finalize( self ):
        """
        Finalize the pool, which closes remaining open connections.
        """


#-------------------------------------------------------------------------------
#
class ConnectionPool(ConnectionPoolInterface):
    """
    A pool of database connections that can be shared by a number of threads.
    """

    _def_minconn = 5
    """The minimum number of connections to keep around."""

    _def_maxconn = None
    """The maximum number of connections to ever allocate (None means that there
    is no limit).  When the maximum is reached, acquiring a new connection is a
    blocking operation."""

    _def_minkeepsecs = 5 # seconds
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
        """The DBAPI-2.0 module interface."""

        self._params = params
        """The parameters for creating a connection."""

        self._pool = []
        self._pool_lock = threading.Condition(threading.RLock())
        """A pool of database connections and an associated lock for access."""

        self._nbconn = 0
        """The total number read-write database connections that were handed
        out.  This does not include the RO connection, if it is created."""

        self._roconn = None
        self._roconn_lock = threading.Lock()
        self._roconn_refs = 0
        """A connection for read-only access and an associated lock for
        creation.  We also store the number of references to it that were
        handled to clients."""

        if options is None:
            options = {}

        self._debug = options.pop('debug', False)
        if self._debug:
            self._log_lock = threading.Lock()
            """Lock used to serialize debug output between threads."""

        self._ro_shared = (not options.pop('disable_ro', False) and
                           dbapi.threadsafety >= 2)
        if not self._ro_shared:
            # Disable the RO connections.
            self.connection_ro = self._connection_ro_crippled

            if not disable_ro and dbapi.threadsafety < 2:
                # Note: Configure with disable_ro to remove this warning
                # message.
                warnings.warn(
                    "Warning: Your DBAPI module '%s' does not support sharing "
                    "connections between threads." % str(dbapi))

        self._minconn = options.pop('minconn', self._def_minconn)

        self._maxconn = options.pop('maxconn', self._def_maxconn)
        if self._maxconn is not None:
            # Reserve one of the available connections for the RO connection.
            if not self._ro_shared:
                self._maxconn -= 1
            assert self._maxconn > 0
            
        self._minkeepsecs = options.pop('minkeepsecs', self._def_minkeepsecs)

        self._user_ro = options.pop('user_readonly', None)
        """User for read-only connections.  You might want to setup different
        privileges for that user in your database configuration."""

        self._debug_unreleased = options.pop('debug_unreleased', None)
        assert (self._debug_unreleased is None or
                isinstance(self._debug_unreleased, types.FunctionType))
                                  
        """Function to call when the connection wrappers are being closed as a
        result of being collected.  This is used to trigger some kind of check
        when you forget to release some connections explicitly."""

        self._isolation_level = options.pop('isolation_level', None)

    def ro_shared( self ):
        """
        Returns true if the read-only connections are shared between the
        threads.
        """
        return self._ro_shared
        
    def module( self ):
        """
        (See base class.)
        """
        return self.dbapi

    def _log( self, msg ):
        """
        Debugging information logging.
        """
        if self._debug:
            self._log_lock.acquire()
            curthread = threading.currentThread()
            log_write('   [%s] %s\n' % (curthread.getName(), msg))
            self._log_lock.release()
        
    def _create_connection( self, read_only ):
        """
        Create a new connection to the database.
        """
        self._log('Connection Create (%s)' % read_only)
        params = self._params
        if read_only and self._user_ro:
            params = params.copy()
            params['user'] = self._user_ro

        newconn = apply(self.dbapi.connect, (), params)
        # Set the isolation level if specified in the options.
        if self._isolation_level is not None:
            newconn.set_isolation_level(self._isolation_level)
        return newconn
        
    def _close( self, conn ):
        """
        Create a new connection to the database.
        """
        self._log('Connection Close')
        return conn.close()

    @staticmethod
    def _add_cursors( conn_wrapper, nbcursors ):
        """
        Return an appropriate value depending on the number of cursors requested
        for a connection wrapper.
        """
        if nbcursors == 0:
            return conn_wrapper
        else:
            r = [conn_wrapper]
            for i in xrange(nbcursors):
                r.append(conn_wrapper.cursor())
            return r

    def _get_connection_ro( self ):
        """
        Acquire a read-only connection.
        """
        self._roconn_lock.acquire()
        self._log('Acquire RO')
        try:
            if not self._roconn:
                self._roconn = self._create_connection(True)
            self._roconn_refs += 1
        finally:
            self._roconn_lock.release()
        return self._roconn

    def connection_ro( self, nbcursors=0 ):
        """
        (See base class.)
        """
        return self._add_cursors(
            ConnectionWrapperRO(self._get_connection_ro(), self), nbcursors)
    
    def _acquire( self ):
        """
        Acquire a connection from the pool, for read an write operations.

        Note that if the maximum number of connections has been reached, this
        becomes a blocking operation.
        """
        self._pool_lock.acquire()
        self._log('Acquire (begin)  Pool: %d  / Created: %s' %
                  (len(self._pool), self._nbconn))
        try:
            # Apply maximum number of connections constraint.
            if self._maxconn is not None:
                # Sanity check.
                assert self._nbconn <= self._maxconn

                while not self._pool and self._nbconn == self._maxconn:
                    # Block until a connection is released.
                    self._log('Acquire (wait)  Pool: %d  / Created: %s' %
                              (len(self._pool), self._nbconn))
                    self._pool_lock.wait()
                    self._log('Acquire (signaled)  Pool: %d  / Created: %s' %
                              (len(self._pool), self._nbconn))

                # Assert that we have a connection in the pool or that we can
                # create a new one if needed, i.e. what we waited for just
                # before.  (This is now a useless sanity check.)
                assert self._pool or self._nbconn < self._maxconn

            if self._pool:
                conn, last_released = self._pool.pop()
            else:
                # Make sure that we never create a new connection if we have
                # reached the maximum.
                if self._maxconn is not None:
                    assert self._nbconn < self._maxconn

                conn = self._create_connection(False)
                self._nbconn += 1

            self._log('Acquire (end  )  Pool: %d  / Created: %s' %
                      (len(self._pool), self._nbconn))
        finally:
            self._pool_lock.release()
        return conn

    def _connection_ro_crippled( self, nbcursors=0 ):
        """
        Replacement for connection_ro() that actually uses the pool to get its
        connections.  This is used when the dbapi does not allow threads to
        share a connection.
        """
        conn = self._acquire()
        return self._add_cursors(ConnectionWrapperCrippled(conn, self),
                                 nbcursors)

    def _get_connection( self ):
        """
        Acquire a read-write connection.
        """
        return self._acquire()

    def connection( self, nbcursors=0, readonly=False ):
        """
        (See base class.)
        """
        if readonly:
            return self.connection_ro(nbcursors)
        return self._add_cursors(
            ConnectionWrapper(self._get_connection(), self), nbcursors)

    def _release_ro( self, conn ):
        """
        Release a reference to the read-only connection.  You should not use
        this directly, you should instead call release() or close() on the
        connection object.
        """
        self._roconn_lock.acquire()
        try:
            assert self._roconn
            assert conn is self._roconn

            self._roconn_refs -= 1
            self._log('Release RO')
        finally:
            self._roconn_lock.release()

    def _release( self, conn ):
        """
        Release a reference to a read-and-write connection.
        """
        self._pool_lock.acquire()
        self._log('Release (begin)  Pool: %d  / Created: %s' %
                  (len(self._pool), self._nbconn))
        try:
            assert conn is not self._roconn
            self._pool.append( (conn, datetime.now()) )
            self._scaledown()
            assert self._pool or self._nbconn < self._maxconn
            self._log('Release (notify)  Pool: %d  / Created: %s' %
                      (len(self._pool), self._nbconn))
            self._pool_lock.notify()
            self._log('Release (notified)  Pool: %d  / Created: %s' %
                      (len(self._pool), self._nbconn))

            self._log('Release (end  )  Pool: %d  / Created: %s' %
                      (len(self._pool), self._nbconn))
        finally:
            self._pool_lock.release()

    def _scaledown( self ):
        """
        Scale down the number of connection according to the following
        heuristic: we want keep a minimum number of extra connections in the
        pool ready for usage.  We delete all connections above that number if
        they have last been used beyond a fixed timeout.
        """
        self._pool_lock.acquire()
        try:
            self._log('Scaledown')

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
                        self._nbconn -= 1
                        n -= 1
                    else:
                        filtered_pool.append(poolitem)
                self._pool = filtered_pool
        finally:
            self._pool_lock.release()

        # Note: we could keep the pool sorted by last_released to minimize the
        # scaledown time, so that the first items in the pool are always the
        # oldest, the most likely to be deletable.

    def finalize( self ):
        """
        Close all the open connections and finalize (prepare for reuse).
        """
        # Make sure that all connections lying about are collected before we go
        # on.
        gc.collect()

        self._roconn_lock.acquire()
        self._pool_lock.acquire()
        try:
            if not self._pool and not self._roconn:
                assert self._nbconn == 0
                return # Already finalized.

            # Check that all the connections have been returned to us.
            assert len(self._pool) == self._nbconn

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
                      (poolsize, self._nbconn))

            # Reset statistics.
            self._nbconn = 0
        finally:
            self._roconn_lock.release()
            self._pool_lock.release()


    def __del__( self ):
        """
        Destructor.
        """
        self.finalize()

    def _getstats( self ):
        """
        Return internal statistics.  This is used for producing graphs depicting
        resource requirements over time.
        """
        total_conn = 0
        self._roconn_lock.acquire()
        try:
            if self._roconn:
                total_conn += 1
        finally:
            self._roconn_lock.release()

        self._pool_lock.acquire()
        try:
            pool_size = len(self._pool)
        finally:
            self._pool_lock.release()
        total_conn += pool_size

        return pool_size, total_conn


#-------------------------------------------------------------------------------
#
class ConnectionWrapperRO(object):
    """
    A wrapper object that behaves like a database connection for read-only
    operations.  You cannot close() this explicitly, you should call release().

    Important: you should always try to explicitly release these objects, in
    order to minimize the number of open connections in the pool.  If you do not
    release explicitly, the pool has to keep the connection open.  Here is the
    preferred way to do this:

       connection = dbpool.connection()
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
            unrel = self._connpool._debug_unreleased
            if unrel:
                unrel(self)
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


class ConnectionWrapperCrippled(ConnectionWrapperRO):
    """
    A wrapper object that releases to the pool.  It still does not provide a
    commit() method however.
    """
    def _release_impl( self, conn ):
        self._connpool._release(conn)

class ConnectionWrapper(ConnectionWrapperCrippled):
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

names = ('martin', 'cyriaque', 'pierre', 'mathieu', 'marie-claude', 'eric'
         'normand', 'christine', 'emric')


class ConnectionPoolPoser(ConnectionPool):
    """
    A fake pool of database connections, that does not pool at all but that
    behaves as if it did.  We use this for implementing performance comparisons
    in the tests.
    """
    def _get_connection_ro( self ):
        return self._create_connection(True)

    def _get_connection( self ):
        return self._create_connection(False)
        
    def _release_ro( self, conn ):
        self._close(conn)
        
    def _release( self, conn ):
        self._close(conn)


class Stats(object):
    """
    An object that has a lock on it that you can use for mut.ex.
    """
    def __init__( self ):
        self._lock = threading.Lock()

        self.ops_ro = 0
        self.ops_rw = 0

    def inc_ops_ro( self ):
        self._lock.acquire()
        self.ops_ro += 1
        self._lock.release()
        
    def inc_ops_rw( self ):
        self._lock.acquire()
        self.ops_rw += 1
        self._lock.release()

class TestThreads(threading.Thread):

    def __init__( self, opts, stats ):
        threading.Thread.__init__(self)

        self.opts = opts
        self.stats = stats
        self._stop = False

    def stop( self ):
        self._stop = True

    def run( self ):
        timeout = (datetime.now() + timedelta(seconds=self.opts.timeout))

        while not self._stop and datetime.now() < timeout:
            time.sleep(random.uniform(0, self.opts.time_wait))

            conn = None
            try:
                if random.random() < self.opts.prob_ro:
                    # Read-only operation.
                    conn = dbpool.connection_ro()

                    curs = conn.cursor()
                    curs.execute("""
                      SELECT name FROM things LIMIT %s;
                      """ % random.randint(0, 5))
                    dbpool._log('SELECT %s\n' % ','.join(
                        map(lambda x: x[0], curs.fetchall())))
                    self.stats.inc_ops_ro()

                else:
                    conn = dbpool.connection()

                    curs = conn.cursor()
                    things = (random.choice(names), self.getName())
                    dbpool._log('INSERT %s\n' % (things,))
                    curs.execute("""
                      INSERT INTO things (name, thread) VALUEs (%s, %s);
                      """, things)
                    conn.commit()
                    self.stats.inc_ops_rw()

            finally:
                time.sleep(self.opts.time_hold)
                if random.random() < self.opts.prob_forget:
                    if conn is not None:
                        conn.release()


def test():
    import optparse
    parser = optparse.OptionParser(__doc__.strip())

    parser.add_option('--debug', action='store_true',
                      help="Enable debugging output.")

    parser.add_option('--threads', '--nb-threads', action='store', type='int',
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

    parser.add_option('-w', '--time-wait', '--wait',
                      action='store', type='float', default=2.0, metavar='SECS',
                      help="Maximum time to wait between each operations.")

    parser.add_option('-H', '--time-hold', '--hold',
                      action='store', type='float', default=0.1, metavar='SECS',
                      help="Time to hold a connection for an operation.")
    
    parser.add_option('-s', '--time-stats', action='store', type='float',
                      default=0.2, metavar='SECS',
                      help="Time to pool the connection pool for statistics. "
                      "This will determine the resolution of the graph "
                      "generated.")

    parser.add_option('--minconn', action='store', type='int',
                      default=3,
                      help="Minimum number of connections to keep around when "
                      "scaling down the pool.")

    parser.add_option('--maxconn', action='store', type='int',
                      default=8,
                      help="Maximum number of connections to create in all.")

    parser.add_option('--minkeepsecs', action='store', type='float',
                      default=5,
                      help="Default seconds to keep a connection for when "
                      "scaling down the pool.")

    parser.add_option('--disable-ro', action='store_true',
                      help="Disable the read-only optimization.")

    parser.add_option('--poser', action='store_true',
                      help="Do not really use connection pooling but rather "
                      "connect and close everytime.")

    parser.add_option('--graph', '--generate-graph', action='store',
                      default=None, metavar='FILE', 
                      help="Generate a graph in the given filename.")

    opts, args = parser.parse_args()

    if opts.graph:
        opts.graph = open(opts.graph, 'w')
        
    poolcls = ConnectionPool
    if opts.poser:
        poolcls = ConnectionPoolPoser
    
    import psycopg2

    options=dict(minconn=opts.minconn,
                 maxconn=opts.maxconn,
                 minkeepsecs=opts.minkeepsecs,
                 disable_ro=opts.disable_ro,
                 debug=opts.debug)

    global dbpool
    dbpool = poolcls(psycopg2,
                     options=options.copy(),
                     database='test',
                     user='blais')

    # Create some tables.
    conn = dbpool.connection()
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

    # Create a global object to accumulate statistics.
    stats = Stats()

    up_and_down = 0
    if up_and_down:
        conns = []
        for i in xrange(10):
            conns.append(dbpool.connection())

        for conn in conns:
            conn.release()

        sys.exit(0)

    # Create threads that operate concurrently on that table.
    threads = []
    for i in xrange(opts.threads):
        t = TestThreads(opts, stats)
        threads.append(t)

    # Start timer.
    time_a = time.time()

    # Start threads.
    for t in threads:
        t.start()
    
    try:
        while time.time() - time_a < opts.timeout:
            time.sleep(opts.time_stats)
            if opts.graph:
                opts.graph.write('%d %d\n' % dbpool._getstats())
    except KeyboardInterrupt:
        print 'Interrupted.'
        for t in threads:
            t.stop()

    for t in threads:
        t.join()

    time_b = time.time()

    dbpool.finalize()

    interval = time_b - time_a
    from pprint import pprint
    print 'Options:'
    for key, value in options.iteritems():
        print '  %s: %s' % (key, value)
    print ('Statistics:  %f RO ops/sec   %f RW ops/sec' % 
           (float(stats.ops_ro)/interval, float(stats.ops_rw)/interval))


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
    import sys, random, time, thread
    log_write = sys.stdout.write
    test()


