import sys
import os
import re
import threading
import sqlite3

from pyrocko.io_common import FileLoadError
from pyrocko.guts import Object, Int, List, String, Timestamp, Dict
from pyrocko import config

from . import model, io
from .client import fdsn


g_databases = {}


def get_database(database=None):
    if isinstance(database, Database):
        return database

    if database is None:
        database = os.path.join(config.config().cache_dir, 'db.squirrel')

    database = os.path.abspath(database)

    if database not in g_databases:
        g_databases[database] = Database(database)

    return g_databases[database]


g_icount = 0
g_lock = threading.Lock()


def make_unique_name():
    with g_lock:
        global g_icount
        name = '%i_%i' % (os.getpid(), g_icount)
        g_icount += 1

    return name


class Selection(object):

    '''
    Database backed file selection.

    :param database: :py:class:`Database` object or path to database or
        ``None`` for user's default database
    :param str persistent: if given a name, create a persistent selection

    By default, a temporary table in the database is created to hold the names
    of the files in the selection. This table is only visible inside the
    application which created it. If a name is given to ``persistent``, a named
    selection is created, which is visible also in other applications using the
    same database. Paths of files can be added to the selection using the
    :py:meth:`add` method.
    '''

    def __init__(self, database=None, persistent=None):
        if database is None and persistent is not None:
            raise Exception(
                'should not use persistent selection with shared global '
                'database as this would impair its performance')

        database = get_database(database)

        if persistent is not None:
            assert isinstance(persistent, str)
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', persistent):
                raise Exception(
                    'invalid persistent selection name: %s' % persistent)

            self.name = 'psel_' + persistent
        else:
            self.name = 'sel_' + make_unique_name()

        self._persistent = persistent is not None
        self._database = database
        self._conn = self._database.get_connection()
        self._sources = []

        self._names = {
            'db': 'main' if self._persistent else 'temp',
            'file_states': self.name + '_file_states',
            'bulkinsert': self.name + '_bulkinsert'}

        self._conn.execute(
            '''
                CREATE TABLE IF NOT EXISTS %(db)s.%(file_states)s (
                    file_id integer PRIMARY KEY,
                    file_state integer)
            ''' % self._names)

    def __del__(self):
        if not self._persistent:
            self._delete()
        else:
            self._conn.commit()

    def get_database(self):
        '''
        Get the database to which this selection belongs.

        :returns: :py:class:`Database` object
        '''
        return self._database

    def _delete(self):
        '''
        Destroy the tables assoctiated with this selection.
        '''
        self._conn.execute(
            'DROP TABLE %(db)s.%(file_states)s' % self._names)

    def add(self, file_paths, state=0):
        '''
        Add files to the selection.

        :param file_paths: Paths to files to be added to the selection.
        :type file_paths: ``list`` of ``str``
        '''

        if isinstance(file_paths, str):
            file_paths = [file_paths]
        self._conn.execute(
            '''
                CREATE TEMP TABLE temp.%(bulkinsert)s
                (path text)
            ''' % self._names)

        self._conn.executemany(
            'INSERT INTO temp.%(bulkinsert)s VALUES (?)' % self._names,
            ((x,) for x in file_paths))

        self._conn.execute(
            '''
                INSERT OR IGNORE INTO files
                SELECT NULL, path, NULL, NULL, NULL
                FROM temp.%(bulkinsert)s
            ''' % self._names)

        self._conn.execute(
            '''
                INSERT OR IGNORE INTO %(db)s.%(file_states)s
                SELECT files.file_id, ?
                FROM temp.%(bulkinsert)s
                INNER JOIN files
                ON temp.%(bulkinsert)s.path == files.path
            ''' % self._names, (state,))

        self._conn.execute(
            'DROP TABLE temp.%(bulkinsert)s' % self._names)

    def remove(self, file_paths):
        '''
        Remove files from the selection.

        :param file_paths: Paths to files to be removed from the selection.
        :type file_paths: ``list`` of ``str``
        '''
        self._conn.executemany(
            '''
                DELETE FROM %(db)s.%(file_states)s
                WHERE %(db)s.%(file_states)s.file_id ==
                    (SELECT files.file_id
                     FROM files
                     WHERE files.path == ?)
            ''' % self._names, ((path,) for path in file_paths))

    def undig_grouped(self, skip_unchanged=False):
        '''
        Get content inventory of all files in selection.

        :param: skip_unchanged: if ``True`` only inventory of modified files
            is yielded (:py:class:`flag_unchanged` must be called beforehand).

        This generator yields tuples ``(path, nuts)`` where ``path`` is the
        path to the file and ``nuts`` is a list of
        :py:class:`pyrocko.squirrel.Nut` objects representing the contents of
        the file.
        '''

        if skip_unchanged:
            where = '''
                WHERE %(db)s.%(file_states)s.file_state == 0
            '''
        else:
            where = ''

        sql = ('''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                nuts.file_segment,
                nuts.file_element,
                kind_codes.kind,
                kind_codes.codes,
                nuts.tmin_seconds,
                nuts.tmin_offset,
                nuts.tmax_seconds,
                nuts.tmax_offset,
                nuts.deltat
            FROM %(db)s.%(file_states)s
            LEFT OUTER JOIN files
                ON %(db)s.%(file_states)s.file_id = files.file_id
            LEFT OUTER JOIN nuts
                ON files.file_id = nuts.file_id
            LEFT OUTER JOIN kind_codes
                ON nuts.kind_codes_id == kind_codes.kind_codes_id
        ''' + where + '''
            ORDER BY %(db)s.%(file_states)s.file_id
        ''') % self._names

        nuts = []
        path = None
        for values in self._conn.execute(sql):
            if path is not None and values[0] != path:
                yield path, nuts
                nuts = []

            if values[1] is not None:
                nuts.append(model.Nut(values_nocheck=values))

            path = values[0]

        if path is not None:
            yield path, nuts

    def flag_unchanged(self, check=True):
        '''
        Mark files which have not been modified.

        :param check: if ``True`` query modification times of known files on
            disk. If ``False``, only flag unknown files.
        '''

        sql = '''
            UPDATE %(db)s.%(file_states)s
            SET file_state = 0
            WHERE (
                SELECT mtime
                FROM files
                WHERE files.file_id == %(db)s.%(file_states)s.file_id) IS NULL
        ''' % self._names

        self._conn.execute(sql)

        if not check:
            return

        def iter_file_states():
            sql = '''
                SELECT
                    files.file_id,
                    files.path,
                    files.format,
                    files.mtime,
                    files.size
                FROM %(db)s.%(file_states)s
                INNER JOIN files
                    ON %(db)s.%(file_states)s.file_id == files.file_id
                WHERE %(db)s.%(file_states)s.file_state != 0
                ORDER BY %(db)s.%(file_states)s.file_id
            ''' % self._names

            for (file_id, path, fmt, mtime_db,
                    size_db) in self._conn.execute(sql):

                try:
                    mod = io.get_backend(fmt)
                    file_stats = mod.get_stats(path)
                except FileLoadError:
                    yield 0, file_id
                    continue
                except io.UnknownFormat:
                    continue

                if (mtime_db, size_db) != file_stats:
                    yield 0, file_id
                    continue

        # could better use callback function here...

        sql = '''
            UPDATE %(db)s.%(file_states)s
            SET file_state = ?
            WHERE file_id = ?
        ''' % self._names

        self._conn.executemany(sql, iter_file_states())


class SquirrelStats(Object):
    '''
    Container to hold statistics about contents available through a squirrel.
    '''

    nfiles = Int.T(
        help='number of files in selection')
    nnuts = Int.T(
        help='number of index nuts in selection')
    codes = List.T(
        List.T(String.T()),
        help='available code sequences in selection, e.g. '
             '(agency, network, station, location) for stations nuts.')
    kinds = List.T(
        String.T(),
        help='available content types in selection')
    total_size = Int.T(
        help='aggregated file size of files is selection')
    counts = Dict.T(
        String.T(), Dict.T(String.T(), Int.T()),
        help='breakdown of how many nuts of any content type and code '
             'sequence are available in selection, ``counts[kind][codes]``')
    tmin = Timestamp.T(
        optional=True,
        help='earliest start time of all nuts in selection')
    tmax = Timestamp.T(
        optional=True,
        help='latest end time of all nuts in selection')


class Squirrel(Selection):
    '''
    Prompt, lazy, indexing, caching, dynamic seismological dataset access.

    :param database: :py:class:`Database` object or path to database or
        ``None`` for user's default database
    :param str persistent: if given a name, create a persistent selection

    By default, temporary tables are created in the attached database to hold
    the names of the files in the selection as well as various indices and
    counters. These tables are only visible inside the application which
    created it. If a name is given to ``persistent``, a named selection is
    created, which is visible also in other applications using the same
    database. Paths of files can be added to the selection using the
    :py:meth:`add` method.
    '''

    def __init__(self, database=None, persistent=None):
        Selection.__init__(self, database=database, persistent=persistent)
        c = self._conn

        self._names.update({
            'nuts': self.name + '_nuts',
            'kind_codes_count': self.name + '_kind_codes_count'})

        c.execute(
            '''
                CREATE TABLE IF NOT EXISTS %(db)s.%(nuts)s (
                    nut_id integer PRIMARY KEY,
                    file_id integer,
                    file_segment integer,
                    file_element integer,
                    kind_codes_id integer,
                    tmin_seconds integer,
                    tmin_offset float,
                    tmax_seconds integer,
                    tmax_offset float,
                    deltat float,
                    kscale integer)
            ''' % self._names)

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS %(db)s.%(nuts)s_file_element
                    ON %(nuts)s (file_id, file_segment, file_element)
            ''' % self._names)

        c.execute(
            '''
                CREATE TABLE IF NOT EXISTS %(db)s.%(kind_codes_count)s (
                    kind_codes_id integer PRIMARY KEY,
                    count integer)
            ''' % self._names)

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_file_id
                ON %(nuts)s (file_id)
            ''' % self._names)

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_tmin_seconds
                ON %(nuts)s (tmin_seconds)
            ''' % self._names)

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_tmax_seconds
                ON %(nuts)s (tmax_seconds)
            ''' % self._names)

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_kscale
                ON %(nuts)s (kscale, tmin_seconds)
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_delete_nuts
                BEFORE DELETE ON main.files FOR EACH ROW
                BEGIN
                  DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_delete_nuts2
                BEFORE UPDATE ON main.files FOR EACH ROW
                BEGIN
                  DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS
                    %(db)s.%(file_states)s_delete_files
                BEFORE DELETE ON %(db)s.%(file_states)s FOR EACH ROW
                BEGIN
                    DELETE FROM %(nuts)s WHERE file_id == old.file_id;
                END
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_inc_kind_codes
                BEFORE INSERT ON %(nuts)s FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO %(kind_codes_count)s VALUES
                    (new.kind_codes_id, 0);
                    UPDATE %(kind_codes_count)s
                    SET count = count + 1
                    WHERE new.kind_codes_id
                        == %(kind_codes_count)s.kind_codes_id;
                END
            ''' % self._names)

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_dec_kind_codes
                BEFORE DELETE ON %(nuts)s FOR EACH ROW
                BEGIN
                    UPDATE %(kind_codes_count)s
                    SET count = count - 1
                    WHERE old.kind_codes_id
                        == %(kind_codes_count)s.kind_codes_id;
                END
            ''' % self._names)

    def _delete(self):
        '''Delete database tables associated with this squirrel.'''

        self._conn.execute(
            'DROP TABLE %(db)s.%(nuts)s' % self._names)

        self._conn.execute(
            'DROP TABLE %(db)s.%(kind_codes_count)s' % self._names)

        Selection._delete(self)

    def _print_tables(self, stream=None):
        if stream is None:
            stream = sys.stdout

        w = stream.write

        w('\n')
        for table in [
                '%(db)s.%(file_states)s',
                '%(db)s.%(nuts)s',
                'files',
                'nuts']:

            w('-' * 64)
            w('\n')
            w(table % self._names)
            w('\n')
            w('-' * 64)
            w('\n')
            sql = ('SELECT * FROM %s' % table) % self._names
            tab = []
            for row in self._conn.execute(sql):
                tab.append([str(x) for x in row])

            widths = [max(len(x) for x in col) for col in zip(*tab)]
            for row in tab:
                w(' '.join(x.ljust(wid) for (x, wid) in zip(row, widths)))
                w('\n')

            w('\n')

    def add(self, file_paths, kinds=None, format='detect', check=True):
        '''
        Add files to the selection.

        :param file_paths: Iterator yielding paths to files to be added to the
            selection.
        :param kinds: if given, allowed content types to be made available
            through the squirrel selection
        :type kinds: ``list`` of ``str``
        :param str format: file format identifier or ``'detect'`` for
            autodetection

        Complexity: O(log N)
        '''
        if isinstance(kinds, str):
            kinds = (kinds,)

        Selection.add(self, file_paths)
        self._load(format, check)
        self._update_nuts(kinds)

    def _load(self, format, check):
        for _ in io.iload(
                self,
                content=[],
                skip_unchanged=True,
                format=format,
                check=check):
            pass

    def _update_nuts(self, kinds):
        c = self._conn
        w_kinds = ''
        args = []
        if kinds is not None:
            w_kinds = 'AND kind_codes.kind IN (%s)' % ', '.join('?'*len(kinds))
            args.extend(kinds)

        c.execute((
            '''
                INSERT INTO %(db)s.%(nuts)s
                SELECT nuts.* FROM %(db)s.%(file_states)s
                INNER JOIN nuts
                    ON %(db)s.%(file_states)s.file_id == nuts.file_id
                INNER JOIN kind_codes
                    ON nuts.kind_codes_id ==
                       kind_codes.kind_codes_id
                WHERE %(db)s.%(file_states)s.file_state != 2
            ''' + w_kinds) % self._names, args)

        c.execute(
            '''
                UPDATE %(db)s.%(file_states)s
                SET file_state = 2
            ''' % self._names)

    def add_fdsn_site(self, site):
        '''
        Add FDSN site for transparent remote data access.
        '''

        self._sources.append(fdsn.FDSNSource(site))

    def undig_span(self, tmin, tmax):
        '''
        Iterate content intersecting with the half open interval [tmin, tmax[.

        :param tmin: timestamp, start time of interval
        :param tmax: timestamp, end time of interval

        Complexity: O(log N)

        Yields :py:class:`pyrocko.squirrel.Nut` objects representing the
        intersecting content.
        '''

        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

        tscale_edges = model.tscale_edges

        tmin_cond = []
        args = []
        for kscale in range(tscale_edges.size + 1):
            if kscale != tscale_edges.size:
                tscale = int(tscale_edges[kscale])
                tmin_cond.append('''
                    (%(db)s.%(nuts)s.kscale == ?
                        AND %(db)s.%(nuts)s.tmin_seconds BETWEEN ? AND ?)
                ''')
                args.extend(
                    (kscale, tmin_seconds - tscale - 1, tmax_seconds + 1))

            else:
                tmin_cond.append('''
                    (%(db)s.%(nuts)s.kscale == ?
                        AND %(db)s.%(nuts)s.tmin_seconds <= ?)
                ''')

                args.extend(
                    (kscale, tmax_seconds + 1))

        sql = ('''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                %(db)s.%(nuts)s.file_segment,
                %(db)s.%(nuts)s.file_element,
                kind_codes.kind,
                kind_codes.codes,
                %(db)s.%(nuts)s.tmin_seconds,
                %(db)s.%(nuts)s.tmin_offset,
                %(db)s.%(nuts)s.tmax_seconds,
                %(db)s.%(nuts)s.tmax_offset,
                %(db)s.%(nuts)s.deltat
            FROM files
            INNER JOIN %(db)s.%(nuts)s
                ON files.file_id == %(db)s.%(nuts)s.file_id
            INNER JOIN kind_codes
                ON %(db)s.%(nuts)s.kind_codes_id == kind_codes.kind_codes_id
            WHERE ( ''' + ' OR '.join(tmin_cond) + ''')
                AND %(db)s.%(nuts)s.tmax_seconds >= ?
        ''') % self._names
        args.append(tmin_seconds)

        for row in self._conn.execute(sql, args):
            nut = model.Nut(values_nocheck=row)
            if nut.tmin < tmax and tmin < nut.tmax:
                yield nut

    def _undig_span_naiv(self, tmin, tmax):
        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

        sql = '''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                %(db)s.%(nuts)s.file_segment,
                %(db)s.%(nuts)s.file_element,
                kind_codes.kind,
                kind_codes.codes,
                %(db)s.%(nuts)s.tmin_seconds,
                %(db)s.%(nuts)s.tmin_offset,
                %(db)s.%(nuts)s.tmax_seconds,
                %(db)s.%(nuts)s.tmax_offset,
                %(db)s.%(nuts)s.deltat
            FROM files
            INNER JOIN %(db)s.%(nuts)s
                ON files.file_id == %(db)s.%(nuts)s.file_id
            INNER JOIN kind_codes
                ON %(db)s.%(nuts)s.kind_codes_id == kind_codes.kind_codes_id
            WHERE %(db)s.%(nuts)s.tmax_seconds >= ?
                AND %(db)s.%(nuts)s.tmin_seconds <= ?
        ''' % self._names

        for row in self._conn.execute(sql, (tmin_seconds, tmax_seconds+1)):
            nut = model.Nut(values_nocheck=row)
            if nut.tmin < tmax and tmin < nut.tmax:
                yield nut

    def time_span(self):
        '''
        Get time interval over all content in selection.

        Complexity O(1), independent of number of nuts

        :returns: (tmin, tmax)
        '''
        sql = '''
            SELECT MIN(tmin_seconds + tmin_offset)
            FROM %(db)s.%(nuts)s WHERE
            tmin_seconds == (SELECT MIN(tmin_seconds) FROM %(db)s.%(nuts)s)
        ''' % self._names
        tmin = None
        for row in self._conn.execute(sql):
            tmin = row[0]

        sql = '''
            SELECT MAX(tmax_seconds + tmax_offset)
            FROM %(db)s.%(nuts)s WHERE
            tmax_seconds == (SELECT MAX(tmax_seconds) FROM %(db)s.%(nuts)s)
        ''' % self._names
        tmax = None
        for row in self._conn.execute(sql):
            tmax = row[0]

        return tmin, tmax

    def iter_kinds(self, codes=None):
        '''
        Iterate over content types available in selection.

        :param codes: if given, get kinds only for selected codes identifier

        Complexity: O(1), independent of number of nuts
        '''

        return self._database._iter_kinds(
            codes=codes,
            kind_codes_count='%(db)s.%(kind_codes_count)s' % self._names)

    def iter_codes(self, kind=None):
        '''
        Iterate over content identifier code sequences available in selection.

        :param kind: if given, get codes only for a given content type

        Complexity: O(1), independent of number of nuts
        '''
        return self._database._iter_codes(
            kind=kind,
            kind_codes_count='%(db)s.%(kind_codes_count)s' % self._names)

    def iter_counts(self, kind=None):
        '''
        Iterate over number of occurences of any (kind, codes) combination.

        :param kind: if given, get counts only for selected content type

        Yields tuples ``((kind, codes), count)``

        Complexity: O(1), independent of number of nuts
        '''
        return self._database._iter_counts(
            kind=kind,
            kind_codes_count='%(db)s.%(kind_codes_count)s' % self._names)

    def get_kinds(self, codes=None):
        '''
        Get content types available in selection.

        :param codes: if given, get kinds only for selected codes identifier

        Complexity: O(1), independent of number of nuts

        :returns: sorted list of available content types
        '''
        return sorted(list(self.iter_kinds(codes=codes)))

    def get_codes(self, kind=None):
        '''
        Get itentifier code sequences available in selection.

        :param kind: if given, get codes only for selected content type

        Complexity: O(1), independent of number of nuts

        :returns: sorted list of available codes
        '''
        return sorted(list(self.iter_codes(kind=kind)))

    def get_counts(self, kind=None):
        '''
        Get number of occurences of any (kind, codes) combination.

        :param kind: if given, get codes only for selected content type

        Complexity: O(1), independent of number of nuts

        :returns: ``dict`` with ``counts[kind][codes] or ``counts[codes]`` 
            if kind is not ``Null``
        '''
        d = {}
        for (k, codes), count in self.iter_counts():
            if k not in d:
                d[k] = {}

            d[k][codes] = count

        if kind is not None:
            return d[kind]
        else:
            return d

    def update_channel_inventory(self, selection):
        for source in self._sources:
            source.update_channel_inventory(selection)
            for path in source.get_channel_file_paths(selection):
                self.add(path)

    def get_nfiles(self):
        '''Get number of files in selection.'''

        sql = '''SELECT COUNT(*) FROM %(db)s.%(file_states)s''' % self._names
        for row in self._conn.execute(sql):
            return row[0]

    def get_nnuts(self):
        '''Get number of nuts in selection.'''
        sql = '''SELECT COUNT(*) FROM %(db)s.%(nuts)s''' % self._names
        for row in self._conn.execute(sql):
            return row[0]

    def get_total_size(self):
        '''Get aggregated file size available in selection.'''
        sql = '''
            SELECT SUM(files.size) FROM %(db)s.%(file_states)s
            INNER JOIN files
                ON %(db)s.%(file_states)s.file_id = files.file_id
        ''' % self._names

        for row in self._conn.execute(sql):
            return row[0]

    def get_stats(self):
        '''
        Get statistics on contents available through this selection.
        '''

        tmin, tmax = self.time_span()

        return SquirrelStats(
            nfiles=self.get_nfiles(),
            nnuts=self.get_nnuts(),
            kinds=self.get_kinds(),
            codes=self.get_codes(),
            total_size=self.get_total_size(),
            counts=self.get_counts(),
            tmin=tmin,
            tmax=tmax)

    def __str__(self):
        return str(self.get_stats())


class DatabaseStats(Object):
    '''
    Container to hold statistics about contents cached in meta-information db.
    '''

    nfiles = Int.T()
    nnuts = Int.T()
    codes = List.T(List.T(String.T()))
    kinds = List.T(String.T())
    total_size = Int.T()
    counts = Dict.T(String.T(), Dict.T(String.T(), Int.T()))


class Database(object):
    '''
    Shared meta-information database used by squirrel.
    '''

    def __init__(self, database_path=':memory:'):
        self._database_path = database_path
        self._conn = sqlite3.connect(database_path)
        self._conn.text_factory = str
        self._initialize_db()
        self._need_commit = False

    def get_connection(self):
        return self._conn

    def _initialize_db(self):
        c = self._conn.cursor()
        c.execute(
            '''PRAGMA recursive_triggers = true''')

        c.execute(
            '''
                CREATE TABLE IF NOT EXISTS files (
                    file_id integer PRIMARY KEY,
                    path text,
                    format text,
                    mtime float,
                    size integer)
            ''')

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS index_files_file_path
                ON files (path)
            ''')

        c.execute(
            '''
                CREATE TABLE IF NOT EXISTS nuts (
                    nut_id integer PRIMARY KEY,
                    file_id integer,
                    file_segment integer,
                    file_element integer,
                    kind_codes_id text,
                    tmin_seconds integer,
                    tmin_offset float,
                    tmax_seconds integer,
                    tmax_offset float,
                    deltat float,
                    kscale integer)
            ''')

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS index_nuts_file_element
                ON nuts (file_id, file_segment, file_element)
            ''')

        c.execute(
            '''
                CREATE TABLE IF NOT EXISTS kind_codes (
                    kind_codes_id integer PRIMARY KEY,
                    kind text,
                    codes text)
            ''')

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS index_kind_codes
                ON kind_codes (kind, codes)
            ''')

        c.execute(
            '''
                CREATE TABLE IF NOT EXISTS kind_codes_count (
                    kind_codes_id integer PRIMARY KEY,
                    count integer)
            ''')

        c.execute(
            '''
                CREATE INDEX IF NOT EXISTS index_nuts_file_id
                ON nuts (file_id)
            ''')

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS delete_nuts_on_delete_file
                BEFORE DELETE ON files FOR EACH ROW
                BEGIN
                  DELETE FROM nuts where file_id == old.file_id;
                END
            ''')

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS delete_nuts_on_update_file
                BEFORE UPDATE ON files FOR EACH ROW
                BEGIN
                  DELETE FROM nuts where file_id == old.file_id;
                END
            ''')

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS increment_kind_codes
                BEFORE INSERT ON nuts FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO kind_codes_count
                    VALUES (new.kind_codes_id, 0);
                    UPDATE kind_codes_count
                    SET count = count + 1
                    WHERE new.kind_codes_id == kind_codes_id;
                END
            ''')

        c.execute(
            '''
                CREATE TRIGGER IF NOT EXISTS decrement_kind_codes
                BEFORE DELETE ON nuts FOR EACH ROW
                BEGIN
                    UPDATE kind_codes_count
                    SET count = count - 1
                    WHERE old.kind_codes_id == kind_codes_id;
                END
            ''')

        self._conn.commit()
        c.close()

    def dig(self, nuts):
        if not nuts:
            return

        c = self._conn.cursor()
        files = set()
        kind_codes = set()
        for nut in nuts:
            files.add((
                nut.file_path,
                nut.file_format,
                nut.file_mtime,
                nut.file_size))
            kind_codes.add((nut.kind, nut.codes))

        c.executemany(
            'INSERT OR IGNORE INTO files VALUES (NULL,?,?,?,?)', files)

        c.executemany(
            '''UPDATE files SET
                format = ?, mtime = ?, size = ?
                WHERE path == ?
            ''',
            ((x[1], x[2], x[3], x[0]) for x in files))

        c.executemany(
            'INSERT OR IGNORE INTO kind_codes VALUES (NULL,?,?)', kind_codes)

        c.executemany(
            '''
                INSERT INTO nuts VALUES
                    (NULL, (
                        SELECT file_id FROM files
                        WHERE path == ?
                     ),?,?,
                     (
                        SELECT kind_codes_id FROM kind_codes
                        WHERE kind == ? AND codes == ?
                     ), ?,?,?,?,?,?)
            ''',
            ((nut.file_path, nut.file_segment, nut.file_element,
              nut.kind, nut.codes,
              nut.tmin_seconds, nut.tmin_offset,
              nut.tmax_seconds, nut.tmax_offset,
              nut.deltat, nut.kscale) for nut in nuts))

        self._need_commit = True
        c.close()

    def undig(self, path):
        sql = '''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                nuts.file_segment,
                nuts.file_element,
                kind_codes.kind,
                kind_codes.codes,
                nuts.tmin_seconds,
                nuts.tmin_offset,
                nuts.tmax_seconds,
                nuts.tmax_offset,
                nuts.deltat
            FROM files
            INNER JOIN nuts ON files.file_id = nuts.file_id
            INNER JOIN kind_codes
                ON nuts.kind_codes_id == kind_codes.kind_codes_id
            WHERE path == ?
        '''

        return [model.Nut(values_nocheck=row)
                for row in self._conn.execute(sql, (path,))]

    def undig_all(self):
        sql = '''
            SELECT
                files.path,
                files.format,
                files.mtime,
                files.size,
                nuts.file_segment,
                nuts.file_element,
                kind_codes.kind,
                kind_codes.codes,
                nuts.tmin_seconds,
                nuts.tmin_offset,
                nuts.tmax_seconds,
                nuts.tmax_offset,
                nuts.deltat
            FROM files
            INNER JOIN nuts ON files.file_id == nuts.file_id
            INNER JOIN kind_codes
                ON nuts.kind_codes_id == kind_codes.kind_codes_id
        '''

        nuts = []
        path = None
        for values in self._conn.execute(sql):
            if path is not None and values[0] != path:
                yield path, nuts
                nuts = []

            if values[1] is not None:
                nuts.append(model.Nut(values_nocheck=values))

            path = values[0]

        if path is not None:
            yield path, nuts

    def undig_many(self, file_paths):
        selection = self.new_selection(file_paths)

        for path, nuts in selection.undig_grouped():
            yield path, nuts

        del selection

    def new_selection(self, file_paths=None, state=0):
        selection = Selection(self)
        if file_paths:
            selection.add(file_paths, state=state)
        return selection

    def commit(self):
        if self._need_commit:
            self._conn.commit()
            self._need_commit = False

    def undig_content(self, nut):
        return None

    def remove(self, path):
        self._conn.execute(
            'DELETE FROM files WHERE path = ?', (path,))

    def reset(self, path):
        self._conn.execute(
            '''
                UPDATE files SET
                    format = NULL,
                    mtime = NULL,
                    size = NULL
                WHERE path = ?
            ''', (path,))

    def _iter_counts(self, kind=None, kind_codes_count='kind_codes_count'):
        args = []
        sel = ''
        if kind is not None:
            sel = 'AND kind_codes.kind == ?'
            args.append(kind)

        sql = ('''
            SELECT
                kind_codes.kind,
                kind_codes.codes,
                %(kind_codes_count)s.count
            FROM %(kind_codes_count)s
            INNER JOIN kind_codes
                ON %(kind_codes_count)s.kind_codes_id
                    == kind_codes.kind_codes_id
            WHERE %(kind_codes_count)s.count > 0
                ''' + sel + '''
        ''') % {'kind_codes_count': kind_codes_count}

        for kind, codes, count in self._conn.execute(sql, args):
            yield (kind, tuple(codes.split('\0'))), count

    def _iter_codes(self, kind=None, kind_codes_count='kind_codes_count'):
        args = []
        sel = ''
        if kind is not None:
            assert isinstance(kind, str)
            sel = 'AND kind_codes.kind == ?'
            args.append(kind)

        sql = ('''
            SELECT DISTINCT kind_codes.codes FROM %(kind_codes_count)s
            INNER JOIN kind_codes
                ON %(kind_codes_count)s.kind_codes_id
                    == kind_codes.kind_codes_id
            WHERE %(kind_codes_count)s.count > 0
                ''' + sel + '''
            ORDER BY kind_codes.codes
        ''') % {'kind_codes_count': kind_codes_count}

        for row in self._conn.execute(sql, args):
            yield tuple(row[0].split('\0'))

    def _iter_kinds(self, codes=None, kind_codes_count='kind_codes_count'):
        args = []
        sel = ''
        if codes is not None:
            assert isinstance(codes, tuple)
            sel = 'AND kind_codes.codes == ?'
            args.append('\0'.join(codes))

        sql = ('''
            SELECT DISTINCT kind_codes.kind FROM %(kind_codes_count)s
            INNER JOIN kind_codes
                ON %(kind_codes_count)s.kind_codes_id
                    == kind_codes.kind_codes_id
            WHERE %(kind_codes_count)s.count > 0
                ''' + sel + '''
            ORDER BY kind_codes.kind
        ''') % {'kind_codes_count': kind_codes_count}

        for row in self._conn.execute(sql, args):
            yield row[0]

    def iter_kinds(self, codes=None):
        return self._iter_kinds(codes=codes)

    def iter_codes(self, kind=None):
        return self._iter_codes(kind=kind)

    def iter_counts(self, kind=None):
        return self._iter_counts(kind=kind)

    def get_kinds(self, codes=None):
        return list(self.iter_kinds(codes=codes))

    def get_codes(self, kind=None):
        return list(self.iter_codes(kind=kind))

    def get_counts(self, kind=None):
        d = {}
        for (k, codes), count in self.iter_counts():
            if k not in d:
                d[k] = {}

            d[k][codes] = count

        if kind is not None:
            return d[kind]
        else:
            return d

    def get_nfiles(self):
        sql = '''SELECT COUNT(*) FROM files'''
        for row in self._conn.execute(sql):
            return row[0]

    def get_nnuts(self):
        sql = '''SELECT COUNT(*) FROM nuts'''
        for row in self._conn.execute(sql):
            return row[0]

    def get_total_size(self):
        sql = '''
            SELECT SUM(files.size) FROM files
        '''

        for row in self._conn.execute(sql):
            return row[0]

    def get_stats(self):
        return DatabaseStats(
            nfiles=self.get_nfiles(),
            nnuts=self.get_nnuts(),
            kinds=self.get_kinds(),
            codes=self.get_codes(),
            counts=self.get_counts(),
            total_size=self.get_total_size())

    def __str__(self):
        return str(self.get_stats())

    def _print_tables(self, stream=None):
        if stream is None:
            stream = sys.stdout

        w = stream.write

        w('\n')
        for table in [
                'files',
                'nuts']:

            w('-' * 64)
            w('\n')
            w(table)
            w('\n')
            w('-' * 64)
            w('\n')
            sql = ('SELECT * FROM %s' % table)
            tab = []
            for row in self._conn.execute(sql):
                tab.append([str(x) for x in row])

            widths = [max(len(x) for x in col) for col in zip(*tab)]
            for row in tab:
                w(' '.join(x.ljust(wid) for (x, wid) in zip(row, widths)))
                w('\n')

            w('\n')


__all__ = [
    'Selection',
    'Squirrel',
    'SquirrelStats',
    'Database',
    'DatabaseStats',
]
