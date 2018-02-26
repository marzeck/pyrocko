import sys
import os
import re
import threading
import sqlite3

from pyrocko.io_common import FileLoadError
from pyrocko.squirrel import model, io
from pyrocko.squirrel.client import fdsn
from pyrocko.guts import Object, Int, List, String, Timestamp
from pyrocko import config


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
            self.delete()
        else:
            self._conn.commit()

    def get_database(self):
        return self._database

    def delete(self):
        self._conn.execute(
            'DROP TABLE %(db)s.%(file_states)s' % self._names)

    def add(self, file_paths, state=0):
        if isinstance(file_paths, str):
            file_paths = [file_paths]
        self._conn.execute(
            '''
                CREATE TEMP TABLE temp.%(bulkinsert)s
                (file_path text)
            ''' % self._names)

        self._conn.executemany(
            'INSERT INTO temp.%(bulkinsert)s VALUES (?)' % self._names,
            ((x,) for x in file_paths))

        self._conn.execute(
            '''
                INSERT OR IGNORE INTO files
                SELECT NULL, file_path, NULL, NULL, NULL
                FROM temp.%(bulkinsert)s
            ''' % self._names)

        self._conn.execute(
            '''
                INSERT OR IGNORE INTO %(db)s.%(file_states)s
                SELECT files.file_id, ?
                FROM temp.%(bulkinsert)s
                INNER JOIN files
                ON temp.%(bulkinsert)s.file_path == files.file_path
            ''' % self._names, (state,))

        self._conn.execute(
            'DROP TABLE temp.%(bulkinsert)s' % self._names)

    def remove(self, file_paths):
        self._conn.executemany(
            '''
                DELETE FROM %(db)s.%(file_states)s
                WHERE %(db)s.%(file_states)s.file_id ==
                    (SELECT files.file_id
                     FROM files
                     WHERE files.file_path == ?)
            ''' % self._names, ((file_path,) for file_path in file_paths))

    def undig_grouped(self, skip_unchanged=False):

        if skip_unchanged:
            where = '''
                WHERE %(db)s.%(file_states)s.file_state == 0
            '''
        else:
            where = ''

        sql = ('''
            SELECT
                files.file_path,
                files.file_format,
                files.file_mtime,
                files.file_size,
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
        file_path = None
        for values in self._conn.execute(sql):
            if file_path is not None and values[0] != file_path:
                yield file_path, nuts
                nuts = []

            if values[1] is not None:
                nuts.append(model.Nut(values_nocheck=values))

            file_path = values[0]

        if file_path is not None:
            yield file_path, nuts

    def flag_unchanged(self, check=True):
        sql = '''
            UPDATE %(db)s.%(file_states)s
            SET file_state = 0
            WHERE (
                SELECT file_mtime
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
                    files.file_path,
                    files.file_format,
                    files.file_mtime,
                    files.file_size
                FROM %(db)s.%(file_states)s
                INNER JOIN files
                    ON %(db)s.%(file_states)s.file_id == files.file_id
                WHERE %(db)s.%(file_states)s.file_state != 0
                ORDER BY %(db)s.%(file_states)s.file_id
            ''' % self._names

            for (file_id, file_path, fmt, mtime_db,
                    size_db) in self._conn.execute(sql):

                try:
                    mod = io.get_format_provider(fmt)
                    file_stats = mod.get_stats(file_path)
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
    nfiles = Int.T()
    nnuts = Int.T()
    codes = List.T(List.T(String.T()))
    kinds = List.T(String.T())
    total_size = Int.T()
    tmin = Timestamp.T(optional=True)
    tmax = Timestamp.T(optional=True)


class Squirrel(Selection):

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

    def delete(self):
        self._conn.execute(
            'DROP TABLE %(db)s.%(nuts)s' % self._names)

        self._conn.execute(
            'DROP TABLE %(db)s.%(kind_codes_count)s' % self._names)

        Selection.delete(self)

    def print_tables(self, stream=None):
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
            w_kinds = 'AND nuts.kind IN (%s)' % ', '.join('?'*len(kinds))
            args.append(kinds)

        c.execute((
            '''
                INSERT INTO %(db)s.%(nuts)s
                SELECT nuts.* FROM %(db)s.%(file_states)s
                INNER JOIN nuts
                    ON %(db)s.%(file_states)s.file_id == nuts.file_id
                WHERE %(db)s.%(file_states)s.file_state != 2
            ''' + w_kinds) % self._names, args)

        c.execute(
            '''
                UPDATE %(db)s.%(file_states)s
                SET file_state = 2
            ''' % self._names)

    def add_fdsn_site(self, site):
        self._sources.append(fdsn.FDSNSource(site))

    def undig_span(self, tmin, tmax):
        '''Get nuts intersecting with the half open interval [tmin, tmax[.'''

        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

        tscale_edges = model.tscale_edges

        tmin_cond = []
        args = []
        for kscale in range(len(tscale_edges) + 1):
            if kscale != len(tscale_edges):
                tscale = tscale_edges[kscale]
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
                files.file_path,
                files.file_format,
                files.file_mtime,
                files.file_size,
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

    def undig_span_naiv(self, tmin, tmax):
        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

        sql = '''
            SELECT
                files.file_path,
                files.file_format,
                files.file_mtime,
                files.file_size,
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

    def tspan(self):
        sql = '''SELECT MIN(tmin_seconds) FROM %(db)s.%(nuts)s''' % self._names
        tmin = None
        for row in self._conn.execute(sql):
            tmin = row[0]

        tmax = None
        sql = '''SELECT MAX(tmax_seconds) FROM %(db)s.%(nuts)s''' % self._names
        for row in self._conn.execute(sql):
            tmax = row[0]

        return tmin, tmax

    def iter_kinds(self, codes=None):
        return self._database._iter_kinds(
            codes=codes,
            kind_codes_count='%(db)s.%(kind_codes_count)s' % self._names)

    def iter_codes(self, kinds=None):
        return self._database._iter_codes(
            kind=kinds,
            kind_codes_count='%(db)s.%(kind_codes_count)s' % self._names)

    def update_channel_inventory(self, selection):
        for source in self._sources:
            source.update_channel_inventory(selection)
            for file_path in source.get_channel_file_paths(selection):
                self.add(file_path)

    def get_nfiles(self):
        sql = '''SELECT COUNT(*) FROM %(db)s.%(file_states)s''' % self._names
        for row in self._conn.execute(sql):
            return row[0]

    def get_nnuts(self):
        sql = '''SELECT COUNT(*) FROM %(db)s.%(nuts)s''' % self._names
        for row in self._conn.execute(sql):
            return row[0]

    def get_total_size(self):
        sql = '''
            SELECT SUM(files.file_size) FROM %(db)s.%(file_states)s
            INNER JOIN files
                ON %(db)s.%(file_states)s.file_id = files.file_id
        ''' % self._names

        for row in self._conn.execute(sql):
            return row[0]

    def get_stats(self):
        return SquirrelStats(
            nfiles=self.get_nfiles(),
            nnuts=self.get_nnuts(),
            kinds=list(self.iter_kinds()),
            codes=list(self.iter_codes()),
            total_size=self.get_total_size())

    def __str__(self):
        return str(self.get_stats())

    def waveform(self, selection=None, **kwargs):
        pass

    def waveforms(self, selection=None, **kwargs):
        pass

    def station(self, selection=None, **kwargs):
        pass

    def stations(self, selection=None, **kwargs):
        self.update_channel_inventory(selection)

    def channel(self, selection=None, **kwargs):
        pass

    def channels(self, selection=None, **kwargs):
        pass

    def response(self, selection=None, **kwargs):
        pass

    def responses(self, selection=None, **kwargs):
        pass

    def event(self, selection=None, **kwargs):
        pass

    def events(self, selection=None, **kwargs):
        pass


class DatabaseStats(Object):
    nfiles = Int.T()
    nnuts = Int.T()
    codes = List.T(List.T(String.T()))
    kinds = List.T(String.T())
    total_size = Int.T()


class Database(object):
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
                    file_path text,
                    file_format text,
                    file_mtime float,
                    file_size integer)
            ''')

        c.execute(
            '''
                CREATE UNIQUE INDEX IF NOT EXISTS index_files_file_path
                ON files (file_path)
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
                file_format = ?, file_mtime = ?, file_size = ?
                WHERE file_path == ?
            ''',
            ((x[1], x[2], x[3], x[0]) for x in files))

        c.executemany(
            'INSERT OR IGNORE INTO kind_codes VALUES (NULL,?,?)', kind_codes)

        c.executemany(
            '''
                INSERT INTO nuts VALUES
                    (NULL, (
                        SELECT file_id FROM files
                        WHERE file_path == ?
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

    def undig(self, file_path):
        sql = '''
            SELECT
                files.file_path,
                files.file_format,
                files.file_mtime,
                files.file_size,
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
            INNER JOIN kind_codes ON nuts.kind_codes_id == kind_codes.kind_codes_id
            WHERE file_path == ?
        '''

        return [model.Nut(values_nocheck=row)
                for row in self._conn.execute(sql, (file_path,))]

    def undig_all(self):
        sql = '''
            SELECT
                files.file_path,
                files.file_format,
                files.file_mtime,
                files.file_size,
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
            INNER JOIN kind_codes ON nuts.kind_codes_id == kind_codes.kind_codes_id
        '''

        nuts = []
        file_path = None
        for values in self._conn.execute(sql):
            if file_path is not None and values[0] != file_path:
                yield file_path, nuts
                nuts = []

            if values[1] is not None:
                nuts.append(model.Nut(values_nocheck=values))

            file_path = values[0]

        if file_path is not None:
            yield file_path, nuts

    def undig_many(self, file_paths):
        selection = self.new_selection(file_paths)

        for file_path, nuts in selection.undig_grouped():
            yield file_path, nuts

        del selection

    def get_file_stats(self, file_paths):
        if isinstance(file_paths, str):
            sql = '''
                SELECT file_mtime, file.file_size
                FROM files
                WHERE file_path = ?
            '''

            for row in self._conn.execute(sql, (file_paths,)):
                return row

            return None
        else:
            selection = self.new_selection(file_paths)
            stats = selection.get_file_stats()
            del selection
            return stats

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

    def remove(self, file_path):
        self._conn.execute(
            'DELETE FROM files WHERE file_path = ?', (file_path,))

    def _iter_codes(self, kind=None, kind_codes_count='kind_codes_count'):
        args = []
        sel = ''
        if kind is not None:
            sel = 'AND kind_codes.codes == ?'
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

        for row in self._conn.execute(sql):
            yield row[0]

    def iter_kinds(self, codes=None):
        return self._iter_kinds(codes=codes)

    def iter_codes(self, kind=None):
        return self._iter_codes(kind=kind)

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
            SELECT SUM(files.file_size) FROM files
        '''

        for row in self._conn.execute(sql):
            return row[0]

    def get_stats(self):
        return DatabaseStats(
            nfiles=self.get_nfiles(),
            nnuts=self.get_nnuts(),
            kinds=list(self.iter_kinds()),
            codes=list(self.iter_codes()),
            total_size=self.get_total_size())

    def __str__(self):
        return str(self.get_stats())

    def print_tables(self, stream=None):
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
            sql = ('SELECT rowid,* FROM %s' % table)
            tab = []
            for row in self._conn.execute(sql):
                tab.append([str(x) for x in row])

            widths = [max(len(x) for x in col) for col in zip(*tab)]
            for row in tab:
                w(' '.join(x.ljust(wid) for (x, wid) in zip(row, widths)))
                w('\n')

            w('\n')


if False:
    sq = Squirrel()
    sq.add('/path/to/data')
#    station = sq.add(Station(...))
#    waveform = sq.add(Waveform(...))

    station = model.Station()
    sq.remove(station)

    stations = sq.stations()
    for waveform in sq.waveforms(stations):
        resp = sq.response(waveform)
        resps = sq.responses(waveform)
        station = sq.station(waveform)
        channel = sq.channel(waveform)
        station = sq.station(channel)
        channels = sq.channels(station)
        responses = sq.responses(channel)
        lat, lon = sq.latlon(waveform)
        lat, lon = sq.latlon(station)
        dist = sq.distance(station, waveform)
        azi = sq.azimuth(channel, station)
