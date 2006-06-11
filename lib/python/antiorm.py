#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
# pylint: disable-msg=W0302

"""
An Anti-ORM, a simple utility functions to ease the writing of SQL statements
with Python DBAPI-2.0 bindings.  This is not an ORM, but it's just as tasty!

This is a only set of support classes that make it easier to write your own
queries yet have some automation with the annoying tasks of setting up lists of
column names and values, as well as doing the type conversions automatically.

And most importantly...

   THERE IS NO FRIGGIN' MAGIC IN HERE.

Some notes:

* The methods defined on the table do not require to have a cursor explicitly
  passed into them, but you can, if you want to.

* There is never any automatic commit performed here, YOU MUST COMMIT BY
  YOURSELF after you've executed the appropriate commands.

Usage
=====

Most of the convenience functions accept a WHERE condition and a tuple or list
of arguments, which are simply passed on to the DBAPI interface.

Initialization
--------------

If you're not going to be passing in your cursors all the time, you need to
provide some kind of source of new cursors to this module::

  # Set default for all tables
  MormTable.engine = MormConnectionEngine(connection)

Declaring Tables
----------------

The table must declare the SQL table's name on the 'table' class attribute and
should derive from MormTable.

You do not need to declare columns on your tables.  However, if you need custom
conversions--right now, only string vs. unicode are useful--you declare a
'converters' mapping from SQL column name to the converter to be used, just for
the columns which require conversion (you can leave others alone).  You can
create your own custom converters if so desired.

The class of objects that are returned by the query methods can be defaulted by
setting 'objcls' on the table.  This class should/may derive from MormObject.


  class TestTable(MormTable):
      table = 'test1'
      objcls = Person
      converters = {
          'firstname': MormConvUnicode(),
          'lastname': MormConvUnicode(),
          'religion': MormConvString()
          }

Insert (C)
----------
Insert some new row in a table::

  TestTable.insert(firstname=u'Adriana',
                   lastname=u'Sousa',
                   religion='candomblé')

Select (R)
----------
Add a where condition, and select some columns::

  for obj in TestTable.select('WHERE id = %s', (2,), cols=('id', 'username')):
      # Access obj.id, obj.username

The simplest version is simply accessing everything::

  for obj in TestTable.select():
      # Access obj.id, obj.username and more.

Update (U)
----------
Update statements are provided as well::

  TestTable.update('WHERE id = %s', (2,),
                   lastname=u'Depardieu',
                   religion='candomblé')

Delete (D)
----------
Deleting rows can be done similarly::

  TestTable.delete('WHERE id = %s', (1,))


Lower-Level APIs
----------------

See the tests at the of this file for examples on how to do things at a
lower-level, which is necessary for complex queries (not that it hurts too much
either).  In particular, you should have a look at the MormDecoder and
MormEncoder classes.


See doc/ in distribution for additional notes.
"""

__author__ = 'Martin Blais <blais@furius.ca>'


__all__ = ['MormTable', 'MormObject', 'MormError',
           'MormConv', 'MormConvUnicode', 'MormConvString',
           'MormDecoder', 'MormEncoder']


#-------------------------------------------------------------------------------
#
class NODEF(object):
    """
    No-defaults constant.
    """

#-------------------------------------------------------------------------------
#
class MormEngine(object):
    """
    Class that provides access to a network connection.  This does *not* manage
    connection pooling at all, it is just a way to create cursors on-the-fly.
    """
    def getcursor(self):
        """
        Get a cursor for read-write operations.
        """
        raise NotImplementedError

    def getcursor_ro(self):
        """
        Get a cursor for read-only operations.
        Override this if you database/connection-pool allows optimized treatment
        of read-only operations.
        """
        # By default, just delegate to the read-write enabled method.
        return self.getcursor()


class MormConnectionEngine(MormEngine):
    """
    Class that provides access to a network connection.  This does *not* manage
    connection pooling at all, it is just a way to create cursors on-the-fly.
    This is meant to be instantiated and set explicitly on the table classes.
    """
    def __init__(self, connection):
        MormEngine.__init__(self)
        self.connection = connection

    def getcursor(self):
        return self.connection.cursor()


#-------------------------------------------------------------------------------
#
class MormObject(object):
    """
    An instance of an initialized decoded row.
    This is just a dummy container for attributes.
    """

#-------------------------------------------------------------------------------
#
class MormTable(object):
    """
    Class for declarations that relate to a table.

    This acts as the base class on which derived classes add custom conversions.
    An instance of this class acts as a wrapper decoder and iterator object,
    whose behaviour depends on the custom converters.
    """

    #---------------------------------------------------------------------------

    table = None
    "Table name in the database."

    pkseq = None
    "Sequence for primary key."

    objcls = MormObject
    "Class of objects to create"

    converters = {}
    "Custom converter map for columns"

    engine = None
    """Engine for automatically getting a connection for making cursors.  If you
    know what you're doing, you could also potentially override this variable
    itself to automatically use the same engine for all the tables."""

    #---------------------------------------------------------------------------

    @classmethod
    def tname(cls):
        assert cls.table is not None
        return cls.table

    @classmethod
    def getengine(cls):
        if cls.engine is None:
            raise MormError("You must set an engine on the Morm table!")
        return cls.engine

    @classmethod
    def encoder(cls, **cols):
        """
        Encode the given columns according to this class' definition.
        """
        return MormEncoder(cls, cols)

    @classmethod
    def decoder(cls, cursor=None):
        """
        Create a decoder for the given column names.
        """
        return MormDecoder(cls, cursor)

    @classmethod
    def decoder_cols(cls, *colnames, **kwds):
        """
        Create a decoder for the given column names.
        """
        cursor = kwds.get('cursor', None)
        return MormDecoder(cls, cursor, colnames)

    @classmethod
    def insert(cls, condstr=None, condargs=None, cursor=None, **fields):
        """
        Convenience method that creates an encoder and executes an insert
        statement.  Returns the encoder.
        """
        enc = cls.encoder(**fields)
        return enc.insert(cursor, condstr, condargs)

    @classmethod
    def create(cls, condstr=None, condargs=None, pk='id', cursor=None,
               **fields):
        """
        Convenience method that creates an encoder and executes an insert
        statement, and then fetches the data back from the database (because of
        defaults) and returns the new object.

        Note: this assumes that the primary key is composed of a single column.
        Note2: this does NOT commit the transaction.
        """
        cls.insert(condstr, condargs, cursor=cursor, **fields)
        pkseq = '%s_%s_seq' % (cls.table, pk)
        seq = cls.getsequence(pkseq=pkseq, cursor=cursor)
        return cls.get(**{pk: seq, 'cursor': cursor})

    @classmethod
    def update(cls, condstr=None, condargs=None, cursor=None, **fields):
        """
        Convenience method that creates an encoder and executes an update
        statement.  Returns the encoder.
        """
        enc = cls.encoder(**fields)
        return enc.update(cursor, condstr, condargs)

    @classmethod
    def select(cls, condstr=None, condargs=None, cols=None, cursor=None):
        """
        Convenience method that executes a select and returns an iterator for
        the results, wrapped in objects with attributes
        """
        if cols is None:
            cols = ()
        decoder = cls.decoder_cols(*cols)
        return decoder.select(cursor, condstr, condargs, cls.objcls)

    @classmethod
    def select_all(cls, condstr=None, condargs=None, cols=None, cursor=None):
        """
        Convenience method that executes a select and returns a list of all the
        results, wrapped in objects with attributes
        """
        if cols is None:
            cols = ()
        decoder = cls.decoder_cols(*cols)
        return decoder.select_all(cursor, condstr, condargs, cls.objcls)

    @classmethod
    def select_one(cls, condstr=None, condargs=None, cols=None, cursor=None):
        """
        Convenience method that executes a select and returns an iterator for
        the results, wrapped in objects with attributes
        """
        it = cls.select(condstr, condargs, cols, cursor)
        if len(it) > 1:
            raise MormError("select_one() matches more than one row.")
        try:
            o = it.next()
        except StopIteration:
            o = None
        return o

    @classmethod
    def get(cls, cols=None, cursor=None, default=NODEF, **constraints):
        """
        Convenience method that gets a single object by its primary key.
        """
        cons, condargs = [], []
        for colname, colvalue in constraints.iteritems():
            cons.append('%s = %%s' % colname)
            condargs.append(colvalue)

        condstr = 'WHERE ' + ' AND '.join(cons)
        it = cls.select(condstr, condargs, cols, cursor)
        if len(it) == 0:
            if default is NODEF:
                raise MormError("Object not found.")
            else:
                return default

        return it.next()

    @classmethod
    def delete(cls, condstr=None, condargs=None, cursor=None):
        """
        Convenience method that deletes rows with the given condition.  WARNING:
        if you do not specify any condition, this deletes all the rows in the
        table!  (just like SQL)
        """
        if condstr is None:
            condstr = ''
        if condargs is None:
            condargs = []

        if cursor is None:
            cursor = cls.getengine().getcursor()

        cursor.execute("DELETE FROM %s %s" % (cls.table, condstr),
                       list(condargs))
        return cursor

    @classmethod
    def getsequence(cls, pkseq=None, cursor=None):
        """
        Return a sequence number.
        This allows us to quickly get the last inserted row id.
        """
        if pkseq is None:
            pkseq = cls.pkseq
            if pkseq is None:
                if cls.table is None:
                    raise MormError("No table specified for "
                                       "getting sequence value")

                # By default use PostgreSQL convention.
                pkseq = '%s_id_seq' % cls.table

        if cursor is None:
            cursor = cls.getengine().getcursor()

        cursor.execute("SELECT currval(%s)", (pkseq,))
        seq = cursor.fetchone()[0]
        return seq


#-------------------------------------------------------------------------------
#
class MormError(Exception):
    """
    Error happening in this module.
    """


#-------------------------------------------------------------------------------
#
class MormConv(object):
    """
    Base class for all automated type converters.
    """
    def from_python(self, value):
        """
        Convert value from Python into a type suitable for insertion in a
        database query.
        """
        return value

    def to_python(self, value):
        """
        Convert value from the type given by the database connection into a
        Python type.
        """
        return value


#-------------------------------------------------------------------------------
#
# Encoding from the DBAPI-2.0 client interface.
dbapi_encoding = 'UTF-8'

class MormConvUnicode(MormConv):
    """
    Conversion between database-encoded string to unicode type.
    """
    def from_python(self, vuni):
        if isinstance(vuni, str):
            vuni = vuni.decode()
        return vuni # Keep as unicode, DBAPI takes care of encoding properly.

    def to_python(self, vstr):
        if vstr is not None:
            return vstr.decode(dbapi_encoding)

class MormConvString(MormConv):
    """
    Conversion between database-encoded string to unicode type.
    """
    # Default value for the desired encoding for the string.
    encoding = 'ISO-8859-1'

    def __init__(self, encoding=None):
        MormConv.__init__(self)
        if encoding:
            self.encoding = encoding
        self.sameenc = (encoding == dbapi_encoding)

    def from_python(self, vuni):
        if isinstance(vuni, str):
            vuni = vuni.decode(self.encoding)
        # Send as unicode, DBAPI takes care of encoding with the appropriate
        # client encoding.
        return vuni

    def to_python(self, vstr):
        if vstr is not None:
            if self.sameenc:
                return vstr
            else:
                return vstr.decode(dbapi_encoding).encode(self.encoding)


#-------------------------------------------------------------------------------
#
class MormEndecBase(object):
    """
    Base class for classes that accept list of tables.
    """
    def __init__(self, tables):

        # Accept multiple formats for tables list.
        self.tables = []
        if not isinstance(tables, (tuple, list)):
            assert issubclass(tables, MormTable)
            tables = (tables,)
        for cls in tables:
            assert issubclass(cls, MormTable)
        self.tables = tuple(tables)
        """Tables is a list of tables that this decoder will use, in order.  You
        can also pass in a single table class, or a sequence of table"""
        assert self.tables

    def table(self):
        return self.tables[0].tname()

    def tablenames(self):
        return ','.join(x.tname() for x in self.tables)

    def getengine(self):
        engine = self.tables[0].getengine()
        assert engine
        return engine


#-------------------------------------------------------------------------------
#
class MormDecoder(MormEndecBase):
    """
    Decoder class that takes care of creating instances with appropriate
    attributes for a specific row.
    """
    def __init__(self, tables, cursor=None, colnames=None):
        MormEndecBase.__init__(self, tables)

        self.colnames = colnames
        """List of column names to restrict decoding.  If this is not specified,
        we will use the list of column names from the cursor."""
        # if colnames is not None: # Remove dotted notation if present.
        #     self.colnames = [c.split('.')[-1] for c in colnames]

        self.cursor = cursor
        """Cursor object, used if there is no set of columns, to figure out
        which columns we should be expecting."""

    def cols(self):
        """
        Return a list of field names, suitable for insertion in a query.
        """
        if not self.colnames:
            return '*'
        else:
            return ', '.join(self.colnames)

    def decode(self, row, obj=None, objcls=None):
        """
        Decode a row.
        """
        if self.colnames:
            if len(self.colnames) != len(row):
                raise MormError("Row has incorrect length for decoder.")
        else:
            if self.cursor is None:
                raise MormError("We need the cursor to decode without desc.")

            # Figure out column names from the cursor.  This is the only reason
            # we need a cursor.
            self.colnames = [x[0] for x in self.cursor.description]

        # Convert all the values right away.  We assume that the query is
        # minimal and that we're going to need to access all the values.
        if obj is None:
            if objcls is not None:
                obj = objcls()
            else:
                obj = MormObject()

        for cname, cvalue in zip(self.colnames, row):
            if '.' in cname:
                # Get the table with the matching name and use the converter on
                # this table if there is one.
                comps = cname.split('.')
                tablename, cname = comps[0], comps[-1]
                for cls in self.tables:
                    if cls.tname() == tablename:
                        converter = cls.converters.get(cname, None)
                        if converter is not None:
                            cvalue = converter.to_python(cvalue)
                        break
            else:
                # Look in the table list for the first appropriate found
                # converter.
                for cls in self.tables:
                    converter = cls.converters.get(cname, None)
                    if converter is not None:
                        cvalue = converter.to_python(cvalue)
                        break

            setattr(obj, cname, cvalue)
        return obj

    def iter(self, cursor=None, objcls=None):
        """
        Create an iterator on the given cursor.
        This also deals with the case where a cursor has no results.
        """
        if cursor is None and self.cursor is None:
            raise MormError("No cursor to iterate.")
        if self.cursor is None:
            self.cursor = cursor
        elif cursor is not None:
            assert self.cursor is cursor
        return MormDecoderIterator(self, objcls)

    def _select(self, cursor=None, condstr=None, condargs=None):
        """
        Guts of the select methods.
        """
        if condstr is None:
            condstr = ''
        if condargs is None:
            condargs = []
        else:
            assert isinstance(condargs, (tuple, list))

        if cursor is None:
            cursor = self.cursor
            if cursor is None:
                # Note: we use just the first table to find an engine.
                cursor = self.getengine().getcursor()
                if cursor is None:
                    raise MormError("No cursor to select.")

        sql = "SELECT %s FROM %s %s" % (self.cols(),
                                        self.tablenames(), condstr)
        cursor.execute(sql, condargs)
        return cursor

    def select(self, cursor=None, condstr=None, condargs=None, objcls=None):
        """
        Execute a select statement and return an iterator for the results.
        """
        curs = self._select(cursor, condstr, condargs)
        return self.iter(curs, objcls)

    def select_all(self, cursor=None, condstr=None, condargs=None,
                   objcls=None):
        """
        Execute a select statement and return all the results directly.
        """
        curs = self._select(cursor, condstr, condargs)

        if curs is None and self.cursor is None:
            raise MormError("No cursor to iterate.")
        if self.cursor is None:
            self.cursor = curs
        else:
            assert self.cursor is cursor

        objects = []
        for row in curs.fetchall():
            objects.append(self.decode(row, objcls=objcls))
        return objects


class MormDecoderIterator(object):
    """
    Iterator for a decoder.
    """
    def __init__(self, decoder, objcls=None):
        self.decoder = decoder
        self.objcls = objcls

    def __len__(self):
        return self.decoder.cursor.rowcount

    def __iter__(self):
        return self

    def next(self, obj=None, objcls=None):
        if self.decoder.cursor.rowcount == 0:
            raise StopIteration

        if objcls is None:
            objcls = self.objcls

        row = self.decoder.cursor.next()
        if row is None:
            raise StopIteration
        else:
            return self.decoder.decode(row, obj, objcls)


#-------------------------------------------------------------------------------
#
class MormEncoder(MormEndecBase):
    """
    Encoder class.  This class converts and contains a set of argument according
    to declared table conversions.  This is mainly used to create INSERT or
    UPDATE statements.
    """
    def __init__(self, tables, fields):
        MormEndecBase.__init__(self, tables)

        self.colnames = []
        """Names of all the columns of the encoder."""

        self.colvalues = []
        """Encoded values of all the fields of the encoder."""

        # Set column names and values, converting if necessary.
        for cname, cvalue in fields.iteritems():
            self.colnames.append(cname)

            # Apply converter to value if necessary
            for cls in self.tables:
                converter = cls.converters.get(cname, None)
                if converter is not None:
                    cvalue = converter.from_python(cvalue)
                    break

            self.colvalues.append(cvalue)

    def cols(self):
        return ', '.join(self.colnames)

    def values(self):
        """
        Returns the list of converted values.
        This is useful to let DBAPI do the automatic quoting.
        """
        return self.colvalues

    def plhold(self):
        """
        Returns a string for holding replacement values in the query string,
        e.g.: %s, %s, %s
        """
        return ', '.join(['%s'] * len(self.colvalues))

    def set(self):
        """
        Returns a string for holding 'set values' syntax in the query string,
        e.g.: col1 = %s, col2 = %s, col3 = %s
        """
        return ', '.join(('%s = %%s' % x) for x in self.colnames)

    def insert(self, cursor, condstr=None, condargs=None):
        """
        Execute a simple insert statement with the contained values.  You can
        only use this on a single table for now.  Note: this does not commit the
        connection.
        """
        assert len(self.tables) == 1
        if condstr is None:
            condstr = ''
        if condargs is None:
            condargs = []

        if cursor is None:
            cursor = self.getengine().getcursor()

        sql = ("INSERT INTO %s (%s) VALUES (%s) %s" %
               (self.table(), self.cols(), self.plhold(), condstr))
        cursor.execute(sql, self.values() + list(condargs))
        return cursor

    def update(self, cursor, condstr=None, condargs=None):
        """
        Execute a simple update statement with the contained values.  You can
        only use this on a single table for now.  Note: this does not commit the
        connection.
        """
        assert len(self.tables) == 1
        if condstr is None:
            condstr = ''
        if condargs is None:
            condargs = []

        if cursor is None:
            cursor = self.getengine().getcursor()

        sql = "UPDATE %s SET %s %s" % (self.table(), self.set(), condstr)
        cursor.execute(sql, self.values() + list(condargs))
        return cursor



#===============================================================================
# TESTS
#===============================================================================

import unittest

class TestMorm(unittest.TestCase):
    """
    Simple automated tests.
    This also acts as examples and documentation.
    """
    conn = None


    def setUp(self):
        # Connect to the database.
        if TestMorm.conn is None:
            import psycopg2
            TestMorm.conn = psycopg2.connect(database='test',
                                         host='localhost',
                                         user='blais',
                                         password='pg')
            self.prepare_testdb()

        self.conn = TestMorm.conn
        the_engine = MormConnectionEngine(self.conn)

        # Declare testing table
        class TestTable(MormTable):
            table = 'test1'
            engine = the_engine
            converters = {
                'firstname': MormConvUnicode(),
                'lastname': MormConvUnicode(),
                'religion': MormConvString()
                }

        # Declare testing table
        class TestTable2(MormTable):
            table = 'test2'
            engine = the_engine
            converters = {
                'motto': MormConvUnicode(),
                }

        self.TestTable = TestTable
        self.TestTable2 = TestTable2


    def prepare_testdb(self):
        """
        Prepare a test database.
        """
        # First drop all existing tables to prepare the test database.
        curs = self.conn.cursor()
        curs.execute("""SELECT table_name FROM information_schema.tables
                          WHERE table_schema = 'public'""")
        for table_name in curs.fetchall():
            curs.execute("DROP TABLE %s" % table_name)

        curs.execute("""

          CREATE TABLE test1 (
            id serial primary key,
            firstname text,
            lastname text,
            religion text,
            creation date
          );

          CREATE TABLE test2 (
            id serial primary key,
            motto text
          )

          """)
        self.conn.commit()


    def test_insert(self):
        """
        Test methods for encoding and for insertion.
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        #
        # Use an explicit encoder object to fill SQL statements.
        #
        enc = TestTable.encoder(firstname=u'Marité',
                                lastname=u'Lubrí',
                                religion='santería')
        curs.execute("INSERT INTO %s (%s) VALUES (%s)" %
                     (enc.table(), enc.cols(), enc.plhold()),
                     enc.values())
        self.conn.commit()

        #
        # INSERT on the encoder object.
        #
        enc = TestTable.encoder(firstname=u'Yanní',
                                lastname=u'Calumà',
                                religion='santería')
        enc.insert(curs)
        self.conn.commit()

        #======================================================================\

        #
        # INSERT on the table (the high-level, normal cases).
        #
        TestTable.insert(firstname=u'Adriana',
                         lastname=u'Sousa',
                         religion='candomblé')
        self.conn.commit()

        #======================================================================/


    def test_select(self):
        """
        Test methods for selecting.
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        #
        # Decode explicitly, using the decoder object.
        #

        # Without restricting column names
        dec = TestTable.decoder(curs)
        curs.execute("SELECT %s FROM %s WHERE religion = %%s" %
                     (dec.cols(), dec.table()), (u'santería',))
        for row in curs:
            self.assert_(dec.decode(row).firstname)
            self.assert_(dec.decode(row).lastname)
            self.assert_(dec.decode(row).religion)

        # With restricting column names.
        dec = TestTable.decoder_cols('firstname')
        curs.execute("SELECT %s FROM %s WHERE religion = %%s" %
                     (dec.cols(), dec.table()), (u'santería',))
        for row in curs:
            self.assert_(dec.decode(row).firstname)

        # With a custom class.
        class MyClass:
            "Dummy class to be created to receive values"
        dec = TestTable.decoder(curs)
        curs.execute("SELECT %s FROM %s WHERE religion = %%s" %
                     (dec.cols(), dec.table()), (u'santería',))
        for row in curs:
            self.assert_(isinstance(dec.decode(row, objcls=MyClass), MyClass))

        # With a custom object.
        myinst = MyClass()
        dec = TestTable.decoder(curs)
        curs.execute("SELECT %s FROM %s WHERE religion = %%s" %
                     (dec.cols(), dec.table()), (u'santería',))
        for row in curs:
            self.assert_(isinstance(dec.decode(row, obj=myinst), MyClass))

        # Using the iterator protocol.
        dec = TestTable.decoder(curs)
        curs.execute("SELECT %s FROM %s WHERE religion = %%s" %
                     (dec.cols(), dec.table()), (u'santería',))
        for obj in dec.iter(curs):
            self.assert_(isinstance(obj, MormObject))

        # Test that it also works with empty results.
        for obj in dec.iter(curs):
            self.assert_(False)

        #
        # SELECT on the decoder object.
        #

        # With condition
        dec = TestTable.decoder()
        it = dec.select(curs, 'WHERE religion = %s', (u'santería',))
        self.assert_(len(it))
        for obj in it:
            self.assert_(obj.firstname)
            self.assert_(obj.lastname)
            self.assert_(obj.religion)

        # Without condition
        for obj in dec.select(curs):
            self.assert_(obj.firstname and obj.lastname and obj.religion)

        # Using cursor on the decoder itself
        dec = TestTable.decoder(curs)
        for obj in dec.select():
            self.assert_(obj.firstname and obj.lastname and obj.religion)

        #======================================================================\

        #
        # SELECT on the table (the high-level, normal cases).
        #

        # Without condition.
        for obj in TestTable.select():
            self.assert_(obj.firstname and obj.lastname and obj.religion)

        # With condition.
        for obj in TestTable.select('WHERE id = %s', (2,)):
            self.assert_(obj.firstname and obj.lastname and obj.religion)

        # With restricted columns.
        it = TestTable.select(cols=('firstname',))
        self.assert_(len(it) == 3)
        for obj in it:
            self.assert_(obj.firstname)
            self.assertRaises(AttributeError, getattr, obj, 'lastname')

        # With dotted names.
        for obj in TestTable.select(cols=('test1.firstname',)):
            self.assert_(obj.firstname)
            self.assertRaises(AttributeError, getattr, obj, 'lastname')

        # Select all.
        it = TestTable.select_all()
        assert it
        for obj in it:
            self.assert_(obj.firstname)

        # Select one
        obj = TestTable.select_one('WHERE id = %s', (42,))
        self.assert_(obj is None)
        self.assertRaises(MormError, TestTable.select_one)
        obj = TestTable.select_one('WHERE id = %s', (2,))
        self.assert_(obj is not None)

        # Empty select.
        it = TestTable.select('WHERE id = %s', (2843732,))
        self.assert_(len(it) == 0)
        self.assert_(not it)
        self.assertRaises(StopIteration, it.next)

        #======================================================================/


    def test_get(self):
        """
        Test methods for getting single objects.
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        # Test succesful get.
        obj = TestTable.get(id=1)
        self.assert_(obj.firstname == u'Marité')

        # Test get failure.
        self.assertRaises(MormError, TestTable.get, id=48337)


    def test_update(self):
        """
        Test methods for modifying existing data.
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        #
        # Use an explicit encoder object to fill SQL statements.
        #
        enc = TestTable.encoder(lastname=u'Blais')
        curs.execute("UPDATE %s SET %s WHERE id = %%s" %
                     (enc.table(), enc.set()),
                     enc.values() + [1])
        self.conn.commit()

        # Check the new value.
        obj = TestTable.select('WHERE id = %s', (1,)).next()
        self.assert_(obj.lastname == 'Blais')

        #
        # UPDATE on the encoder object.
        #
        enc = TestTable.encoder(lastname=u'Binoche')
        enc.update(curs, 'WHERE id = %s', (1,))
        self.conn.commit()

        # Check the new value.
        obj = TestTable.select('WHERE id = %s', (1,)).next()
        self.assert_(obj.lastname == 'Binoche')

        #======================================================================\

        #
        # UPDATE on the table (the high-level, normal cases).
        #
        TestTable.update('WHERE id = %s', (2,),
                         lastname=u'Depardieu',
                         religion='candomblé')
        self.conn.commit()

        # Check the new value.
        obj = TestTable.select('WHERE id = %s', (2,)).next()
        self.assert_(obj.lastname == 'Depardieu')

        #======================================================================/


    def test_delete(self):
        """
        Test methods for deleting.
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        it = TestTable.select()
        self.assert_(len(it) == 3)

        #======================================================================\

        #
        # DELETE from the table.
        #
        TestTable.delete('WHERE id = %s', (1,))
        it = TestTable.select()
        self.assert_(len(it) == 2)

        TestTable.delete()
        it = TestTable.select()
        self.assert_(len(it) == 0)

        #======================================================================/


    def test_date(self):
        """
        Test storing and reading back a date.
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        TestTable.insert(firstname=u'Gérard',
                         lastname=u'Depardieu',
                         religion='christian')
        self.conn.commit()

        # Check date type.
        import datetime
        today = datetime.date.today()

        TestTable.update('WHERE lastname = %s', ('Depardieu',),
                         creation=today)
        self.conn.commit()

        it = TestTable.select('WHERE lastname = %s', ('Depardieu',))
        obj = it.next()
        self.assert_(isinstance(obj.creation, datetime.date))


    def test_conversions(self):
        """
        Test some type conversions.
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        # Check unicode string type.
        obj = TestTable.get(id=4)
        self.assert_(isinstance(obj.firstname, unicode))
        self.assert_(obj.firstname == u'Gérard')
        self.assert_(isinstance(obj.lastname, unicode))
        self.assert_(obj.lastname == u'Depardieu')
        self.assert_(isinstance(obj.religion, str))
        self.assert_(obj.religion == u'christian'.encode('latin-1'))


    def test_sequence(self):
        """
        Test methods for encoding and for insertion.
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        TestTable.insert(firstname=u'Rachel',
                         lastname=u'Lieblein-Harrod',
                         religion='jewish')
        self.conn.commit()

        #======================================================================\

        # Get the sequence number for the last insertion.
        seq = TestTable.getsequence()

        obj = TestTable.select('WHERE id = %s', (seq,)).next()
        self.assert_(obj.firstname == 'Rachel')

        #======================================================================/


    def test_create(self):
        """
        Test methods for creation (insertion and then getting at the object).
        """
        curs, TestTable = self.conn.cursor(), self.TestTable

        #======================================================================\

        obj = TestTable.create(firstname=u'Hughes',
                               lastname=u'Leblanc',
                               religion='blackmagic')
        self.conn.commit()

        self.assert_(obj.lastname == 'Leblanc')

        #======================================================================/


    def test_multi_tables(self):
        """
        Test query on multiple tables with conversion.
        """
        curs = self.conn.cursor()
        TestTable, TestTable2 = self.TestTable, self.TestTable2

        dec = MormDecoder((TestTable, TestTable2), curs)

        TestTable2.insert(id=1, motto=u"I don't want any, ok?")
        self.conn.commit()

        curs.execute("""
            SELECT firstname, lastname, motto FROM %s
              WHERE test1.id = test2.id
            """ % dec.tablenames())
        it = dec.iter(curs)
        assert it
        for o in it:
            assert o.firstname and o.lastname and o.motto
            assert isinstance(o.firstname, unicode)
            assert isinstance(o.motto, unicode)


class TestMormGlobal(TestMorm):
    """
    Simple automated tests.
    This also acts as examples and documentation.
    """
    def setUp(self):
        TestMorm.setUp(self)
        MormTable.engine = self.TestTable.engine
        del self.TestTable.engine

    test_insert = TestMorm.test_insert


def suite():
    thesuite = unittest.TestSuite()
    thesuite.addTest(TestMorm("test_insert"))
    thesuite.addTest(TestMorm("test_select"))
    thesuite.addTest(TestMorm("test_multi_tables"))
    thesuite.addTest(TestMorm("test_get"))
    thesuite.addTest(TestMorm("test_update"))
    thesuite.addTest(TestMorm("test_delete"))
    thesuite.addTest(TestMorm("test_date"))
    thesuite.addTest(TestMorm("test_conversions"))
    thesuite.addTest(TestMorm("test_sequence"))
    thesuite.addTest(TestMorm("test_create"))
    thesuite.addTest(TestMormGlobal("test_insert"))
    return thesuite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')

