#!/usr/bin/env python
"""
An extention to DBAPI-2.0 for the easier building of SQL statements.

This extension allows you to call a DBAPI Cursor's execute method with a string
that contains format specifiers for escaped and/or unescaped arguments.  Escaped
arguments are specified using `` %S `` (capital S).  You can also mix positional
and keyword arguments in the call, and this takes advantage of the Python call
syntax niceties.  Also, lists passed in as parameters to be formatted are
automatically joined by commas (this works for both unescaped and escaped
parameters-- lists to be escaped have their elements escaped individually).

For performance, the results of analysing and preparing the query is kept in a
cache and reused on subsequence calls, similarly to the re or struct library.

(This is intended to become a reference implementation for a proposal for an
extension to tbe DBAPI-2.0.)

.. note:: for now the transformation only works with DBAPIs that supports
          parametric arguments in the form of Python's syntax for now
          (e.g. psycopg2).  It could easily be extended to support other DBAPI
          syntaxes.

For more details and motivation, see the accompanying explanation document at
http://furius.ca/pubcode/pub/conf/common/lib/python/dbapiext.html

5-minute usage instructions:

  Run execute_f() with a cursor object and appropriate arguments::

    execute_f(cursor, ' SELECT %s FROM %(t)s WHERE id = %S ', cols, id, t=table)

  Ideally, we should be able to monkey-patch this method onto the cursor class
  of the DBAPI library (this may not be possible if it is an extension module).

  By default, the result of analyzing each query is cached automatically and
  reused on further invocations, to minimize the amount of analysis to be
  performed at runtime.  If you want to do this explicitly, first compile your
  query, and execute it later with the resulting object, e.g.::

    analq = qcompile(' SELECT %s FROM %s WHERE id = %S ')
    ...
    analq.execute(cursor, cols, id, t=table)

**Note to developers: this module contains tests, if you make any changes,
please make sure to run and fix the tests.**

Future Work
===========

- We could provide a reduce() method on the QueryAnalyzer, that will apply the
  given parameters and save the calculated arguments for later use; This would
  allow us to apply queries using multiple calls, to fill in only certain
  parameters at a time.  This method would return a new QueryAnalyzer, albeit
  one that would contain some pre-cooked apply_kwds and delay_kwds to be
  accumulated to in the apply call.

"""

# stdlib imports
import re
from StringIO import StringIO
from datetime import date, datetime
from itertools import izip, count
from pprint import pprint, pformat


__all__ = ('execute_f', 'qcompile', 'set_paramstyle')


class QueryAnalyzer(object):
    """
    Analyze and contain a query string in a way that we can quickly put it back
    together when given the actual arguments.  This object contains knowledge of
    which arguments are positional and keyword, and is able to conditionally
    apply escaping when necessary, and expand lists as well.

    This is meant to be kept around or cached for efficiency.
    """

    # Note: the 'S' formatting character is extra, from us.
    re_fmt = '[#0 +-]?([0-9]+|\\*)?(\\.[0-9]*)?[hlL]?[diouxXeEfFgGcrsS]'

    regexp = re.compile('%%(\\(([a-zA-Z0-9_]+)\\))?(%s)' % re_fmt)

    def __init__(self, query, paramstyle=None):
        self.orig_query = query

        self.positional = []
        """List of positional arguments to be consumed later.  The list consists
        of keynames."""

        self.components = None
        "A sequence of strings or match objects."

        if paramstyle is None:
            paramstyle = _def_paramstyle
        self.paramstyle = paramstyle
        self.init_style(paramstyle)
        "The parameter style supported by the underlying DBAPI."

        self.analyze() # Initialize.

    def init_style(self, paramstyle):
        "Pre-calculate style-specific constants."
        if paramstyle == 'pyformat':
            self.style_fmt = '%%%%(%(name)s)s'
            self.style_argstype = dict
        elif paramstyle == 'named':
            self.style_fmt = ':%(name)s'
            self.style_argstype = dict
        elif paramstyle == 'qmark':
            self.style_fmt = '?'
            self.style_argstype = list
        elif paramstyle == 'format':
            self.style_fmt = '%%%%s'
            self.style_argstype = list
        elif paramstyle == 'numeric':
            self.style_fmt = ':%(no)d'
            self.style_argstype = list
        # Non-standard. For our modified Sybase (from 0.37).
        elif paramstyle == 'atnamed':
            self.style_fmt = '@%(name)s'
            self.style_argstype = dict
        else:
            raise ValueError(
                "Parameter style '%s' is not supported." % paramstyle)

    def analyze(self):
        query = self.orig_query

        poscount = count(1)

        comps = self.components = []
        for x in gensplit(self.regexp, query):
            if isinstance(x, str):
                comps.append(x)
            else:
                keyname, fmt = x.group(2, 3)
                if keyname is None:
                    keyname = '__p%d' % poscount.next()
                    self.positional.append(keyname)
                if fmt == 'S':
                    fmt = 's'
                    escaped = True
                else:
                    escaped = False
                comps.append( (keyname, escaped, fmt) )

    def __str__(self):
        """
        Return the string that would be used before application of the
        positional and keyword arguments.
        """
        style_fmt = self.style_fmt
        oss = StringIO()
        no = count(1)
        for x in self.components:
            if isinstance(x, str):
                oss.write(x)
            else:
                keyname, escaped, fmt = x
                if escaped:
                    oss.write(style_fmt % {'name': keyname,
                                           'no': no.next()})
                else:
                    oss.write('%%(%s)%s' % (keyname, fmt))
        return oss.getvalue()
    
    def apply(self, *args, **kwds):
        if len(args) != len(self.positional):
            raise TypeError('not enough arguments for format string')

        # Merge the positional arguments in the keywords dict.
        for name, value in izip(self.positional, args):
            assert name not in kwds
            kwds[name] = value

        # Patch up the components into a string.
        listexpans = {} # cached list expansions.
        apply_kwds, delay_kwds = {}, self.style_argstype()

        no = count(1)
        style_fmt = self.style_fmt
        output = []
        for x in self.components:
            if isinstance(x, str):
                out = x
            else:
                keyname, escaped, fmt = x

                # Split keyword lists.
                # Expand into lists of words.
                value = kwds[keyname]
                if isinstance(value, (tuple, list)):
                    try:
                        words = listexpans[keyname] # Try cache.
                    except KeyError:
                        # Compute list expansion.
                        words = ['%s_l%d__' % (keyname, x)
                                 for x in xrange(len(value))]
                        listexpans[keyname] = words
                else:
                    words, value = (keyname,), (value,)

                if escaped:
                    okwds = delay_kwds
                    outfmt = [style_fmt %
                              {'name': x, 'no': no.next()} for x in words]
                else:
                    okwds = apply_kwds
                    outfmt = ['%%(%s)%s' % (x, fmt) for x in words]

                # Dispatch values on the appropriate output dictionary.
                assert len(words) == len(value)
                if isinstance(okwds, dict):
                    okwds.update(izip(words, value))
                else:
                    okwds.extend(value)

                # Create formatting string.
                out = ','.join(outfmt)

            output.append(out)

        # Apply positional arguments, here, now.
        newquery = ''.join(output)

        # Return the string with the delayed arguments as formatting specifiers,
        # to be formatted by DBAPI, and the delayed arguments.
        return newquery % apply_kwds, delay_kwds

    def execute(self, cursor_, *args, **kwds):
        """
        Execute the analyzed query on the given cursor, with the given arguments
        and keywords.
        """
        # Translate this call into a compatible call to execute().
        cquery, ckwds = self.apply(*args, **kwds)

        # Execute the transformed query.
        return cursor_.execute(cquery, ckwds)


def gensplit(regexp, s):
    """
    Regexp-splitter generator.  Generates strings and match objects.
    """
    c = 0
    for mo in regexp.finditer(s):
        yield s[c:mo.start()]
        yield mo
        c = mo.end()
    yield s[c:]


#-------------------------------------------------------------------------------
#
_def_paramstyle = 'pyformat'

def set_paramstyle(style_or_dbapi):
    """
    Sets the default paramstyle to be used by the underlying DBAPI.
    You can pass in a DBAPI module object or a string. See PEP249 for details.
    """
    global _def_paramstyle
    if isinstance(style_or_dbapi, str):
        _def_paramstyle = style_or_dbapi
    else:
        _def_paramstyle = style_or_dbapi.paramstyle
    assert _def_paramstyle in ('qmark', 'numeric',
                               'named', 'format', 'pyformat')


#-------------------------------------------------------------------------------
#
qcompile = QueryAnalyzer
"""
Compile a query in a compatible query analyzer.
"""


#-------------------------------------------------------------------------------
#
# Query cache used to avoid having to analyze the same queries multiple times.
# Hashed on the query string.
_query_cache = {}

# Note: we use cursor_ and query_ because we often call this function with
# vars() which include those names on the caller side.
def execute_f(cursor_, query_, *args, **kwds):
    """
    Fancy execute method for a cursor.  (Note: this is implemented as a function
    but is really meant to be a method to replace or complement the standard
    method Cursor.execute() from DBAPI-2.0.)

    Convert fancy query arguments into a DBAPI-compatible set of arguments and
    execute.

    This method supports a different syntax than the DBAPI execute() method:

    - By default, %s placeholders are not escaped.

    - Use the %S or %(name)S placeholder to specify escaped strings.

    - You can specify positional arguments without having to place them in an
      extra tuple.

    - Keyword arguments are used as expected to fill in missing values.
      Positional arguments are used to fill non-keyword placeholders.

    - Arguments that are tuples or lists will be automatically joined by colons.
      If the corresponding formatting is %S or %(name)S, the members of the
      sequence will be escaped individually.

    See qcompile() for details.

    Note that this function accepts a '_paramstyle' optional argument, to set
    which parameter style to use.
    """
    if debug_convert:
        print '\n' + '=' * 80
        print '\noriginal ='
        print query_
        print '\nargs ='
        pprint(args)
        print '\nkwds ='
        pprint(kwds)

    # Get the cached query analyzer or create one.
    try:
        q = _query_cache[query_]
    except KeyError:
        _query_cache[query_] = q = qcompile(
            query_,
            paramstyle=kwds.pop('paramstyle', None))

    if debug_convert:
        print '\nquery analyzer =', str(q)

    # Translate this call into a compatible call to execute().
    cquery, ckwds = q.apply(*args, **kwds)

    if debug_convert:
        print '\ntransformed ='
        print cquery
        print '\nnewkwds ='
        pprint(ckwds)

    # Execute the transformed query.
    return cursor_.execute(cquery, ckwds)


#===============================================================================
# TESTS

import unittest

class TestCursor(object):
    """
    Fake cursor that fakes the escaped replacments like a real DBAPI cursor, but
    simply returns the final string.
    """
    execute_f = execute_f

    def execute(self, query, args):
        return self.render_fake(query, args).strip()

    @staticmethod
    def render_fake(query, kwds):
        """
        Take arguments as the DBAPI of execute() accepts and fake escaping the
        arguments as the DBAPI implementation would and return the resulting
        string.  This is used only for testing, to make testing easier and more
        intuitive, to view the completed queries without the replacement
        variables.
        """
        for key, value in kwds.items():
            if isinstance(value, str):
                kwds[key] = repr(value)
            elif isinstance(value, unicode):
                kwds[key] = repr(value.encode('utf-8'))
            elif isinstance(value, (date, datetime)):
                kwds[key] = repr(value.isoformat())

        result = query % kwds

        if debug_convert:
            print '\n--- 5. after full replacement (fake dbapi application)'
            print result

        return result


class TestExtension(unittest.TestCase):
    """
    Tests for the extention functions.
    """
    def compare_nows(self, s1, s2):
        """
        Compare two strings without considering the whitespace.
        """
        s1 = s1.replace(' ', '').replace('\n', '')
        s2 = s2.replace(' ', '').replace('\n', '')
        self.assertEquals(s1, s2)

    def test_basic(self):
        "Basic replacement tests."

        cursor = TestCursor()

        simple, isimple, seq = 'SIMPLE', 42, ('L1', 'L2', 'L3')
        for query, args, kwds, expect in (

            # With simple arguments.
            (' %s ', (simple,), dict(), " SIMPLE "),
            (' %S ', (simple,), dict(), " 'SIMPLE' "),
            (' %d ', (isimple,), dict(), " 42 "),
            (' %(k)s ', (), dict(k=simple), " SIMPLE "),
            (' %(k)d ', (), dict(k=isimple), " 42 "),
            (' %(k)S ', (), dict(k=simple), " 'SIMPLE' "),

            # Same but with lists.
            (' %s ', (seq,), dict(), " L1,L2,L3 "),
            (' %S ', (seq,), dict(), " 'L1','L2','L3' "),
            (' %(k)s ', (), dict(k=seq), " L1,L2,L3 "),
            (' %(k)S ', (), dict(k=seq), " 'L1','L2','L3' "),

            ):

            # Normal invocation.
            self.compare_nows(
                cursor.execute_f(query, *args, **kwds),
                expect)

            # Repeated destination formatting string.
            self.compare_nows(
                cursor.execute_f(query + query, *(args + args) , **kwds),
                expect + expect)


    def test_misc(self):

        d = date(2006, 07, 28)

        cursor = TestCursor()

        self.compare_nows(
            cursor.execute_f('''
              INSERT INTO %(table)s (%s)
                SET VALUES (%S)
                WHERE id = %(id)S
                  AND name IN (%(name)S)
                  AND name NOT IN (%(name)S)
            ''',
                         ('col1', 'col2'),
                         (42, "bli"),
                         id="02351440-7b7e-4260",
                         name=[45, 56, 67, 78],
                         table='table'),
              """
              INSERT INTO table (col1, col2)
                SET VALUES (42, 'bli')
                WHERE id = '02351440-7b7e-4260'
                  AND name IN (45, 56, 67, 78)
                  AND name NOT IN (45, 56, 67, 78)
              """)


        # Note: this should fail in the old text.
        self.compare_nows(
            cursor.execute_f(''' %(id)s AND %(id)S ''',
                         id=['fulano', 'mengano']),
              """ fulano,mengano AND 'fulano','mengano' """)


        self.compare_nows(
            cursor.execute_f('''
              SELECT %s FROM %s WHERE id = %S
            ''',
                         ('id', 'name', 'title'), 'books',
                         '02351440-7b7e-4260'),
            """SELECT id,name,title FROM books
               WHERE id = '02351440-7b7e-4260'""")

        self.compare_nows(
            cursor.execute_f('''
           SELECT %s FROM %s WHERE id = %(id)S %(id)S
        ''', ('id', 'name', 'title'), 'books', id=d),
            """SELECT id,name,title FROM books
               WHERE id = '2006-07-28' '2006-07-28'""")

        self.compare_nows(
            cursor.execute_f(''' %(id)S %(id)S ''', id='02351440-7b7e-4260'),
            " '02351440-7b7e-4260' '02351440-7b7e-4260' ")

        self.compare_nows(
            cursor.execute_f(''' %s %(id)S %(id)s ''',
                         'books',
                         id='02351440-7b7e-4260'),
            "  books '02351440-7b7e-4260' 02351440-7b7e-4260  ")

        self.compare_nows(
            cursor.execute_f('''
              SELECT %s FROM %(table)s WHERE col1 = %S AND col2 < %(val)S
            ''', ('col1', 'col2', 'col3'), 'value1', table='my-table', val=42),
            """ SELECT col1,col2,col3 FROM my-table
                WHERE col1 = 'value1' AND col2 < 42 """)

        self.compare_nows(
            cursor.execute_f("""
              INSERT INTO thumbnails
                (basename, photo1, photo2, photo3)
                VALUES (%S, %S)
                """, 'PHOTONAME', ('BIN1', 'BIN2', 'BIN3')),
            """
              INSERT INTO thumbnails
                (basename, photo1, photo2, photo3)
                VALUES ('PHOTONAME', 'BIN1', 'BIN2', 'BIN3')
                """)


    def test_paramstyles(self):

        d = date(2006, 07, 28)

        cursor = TestCursor()

        query = '''
              Simple: %s  Escaped: %S
              Kwd: %(bli)s KwdEscaped: %(bli)S  
            '''
        args = ('hansel', 'gretel')
        kwds = dict(bli='bethel')

        test_data = {
            'pyformat': ("""
              Simple: hansel  Escaped: %(__p2)s
              Kwd: bethel KwdEscaped: %(bli)s
            """, {'__p2': 'gretel', 'bli': 'bethel'}),
            
            'named': ("""
              Simple: hansel  Escaped: :__p2
              Kwd: bethel KwdEscaped: :bli
            """, {'__p2': 'gretel', 'bli': 'bethel'}),

            'qmark': ("""
              Simple: hansel  Escaped: ?
              Kwd: bethel KwdEscaped: ?
            """, ['gretel', 'bethel']),

            'format': ("""
              Simple: hansel  Escaped: %s
              Kwd: bethel KwdEscaped: %s
            """, ['gretel', 'bethel']),

            'numeric': ("""
              Simple: hansel  Escaped: :1
              Kwd: bethel KwdEscaped: :2
            """, ['gretel', 'bethel']),
            }

        for style, (estr, eargs) in test_data.iteritems():
            qstr, qargs = qcompile(query, paramstyle=style).apply(*args, **kwds)

            self.compare_nows(qstr, estr)
            self.assertEquals(qargs, eargs)

        # Visual debugging.
        print_it = 0
        for style in test_data.iterkeys():
            qanal = qcompile("""
              %S %(c1)S %S %S %(c2)S
            """, paramstyle=style)

            qstr, qargs = qanal.apply(1, 2, 3, c1='CC1', c2='CC2')
            if print_it:
                print qstr
                print qargs

debug_convert = 0
if __name__ == '__main__':
    unittest.main()


