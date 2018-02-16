import sys
import os
import re
import threading
import sqlite3
from collections import defaultdict, Counter

from pyrocko.io_common import FileLoadError
from pyrocko.squirrel import model, io
from pyrocko.squirrel.client import fdsn
from pyrocko.guts import Object, Int, List, String
from pyrocko import config


def iitems(d):
    try:
        return d.iteritems()
    except AttributeError:
        return d.items()


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
                'database as this would decrease its performance')

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
            'file_states': self.name + '_file_states'}

        self._conn.execute(
            '''CREATE TABLE IF NOT EXISTS %(db)s.%(file_states)s (
                file_name text PRIMARY KEY,
                file_state integer)''' % self._names)

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

    def add(self, filenames, state=0):
        if isinstance(filenames, str):
            filenames = [filenames]
        self._conn.executemany(
            'INSERT OR IGNORE INTO %(db)s.%(file_states)s VALUES (?, ?)'
            % self._names,
            ((s, state) for s in filenames))

    def undig_grouped(self, skip_unchanged=False):

        if skip_unchanged:
            where = '''
                WHERE %(db)s.%(file_states)s.file_state == 0
            '''
        else:
            where = ''

        sql = ('''
            SELECT
                %(db)s.%(file_states)s.file_name,
                files.file_name,
                files.file_format,
                files.file_mtime,
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
            ON %(db)s.%(file_states)s.file_name = files.file_name
            LEFT OUTER JOIN nuts
                ON files.rowid = nuts.file_id
            LEFT OUTER JOIN kind_codes
                ON nuts.kind_codes_id == kind_codes.rowid
        ''' + where + '''
            ORDER BY %(db)s.%(file_states)s.rowid
        ''') % self._names

        nuts = []
        fn = None
        for values in self._conn.execute(sql):
            if fn is not None and values[0] != fn:
                yield fn, nuts
                nuts = []

            if values[1] is not None:
                nuts.append(model.Nut(values_nocheck=values[1:]))

            fn = values[0]

        if fn is not None:
            yield fn, nuts

    def iter_mtimes(self):
        sql = '''
            SELECT
                files.file_name,
                files.file_format,
                files.file_mtime
            FROM %(db)s.%(file_states)s
            LEFT OUTER JOIN files
            ON %(db)s.%(file_states)s.file_name = files.file_name
            ORDER BY %(db)s.%(file_states)s.rowid
        ''' % self._names

        for row in self._conn.execute(sql):
            yield row

    def get_mtimes(self):
        return list(mtime for (_, _, mtime) in self.iter_mtimes())

    def flag_unchanged(self, check_mtime=True):

        def iter_filenames_states():
            for filename, fmt, mtime_db in self.iter_mtimes():
                if mtime_db is None or not os.path.exists(filename):
                    yield 0, filename
                    continue

                if check_mtime:
                    try:
                        mod = io.get_format_provider(fmt)
                        mtime_file = mod.get_mtime(filename)
                    except FileLoadError:
                        yield 0, filename
                        continue
                    except io.UnknownFormat:
                        continue

                    if mtime_db != mtime_file:
                        yield 0, filename
                        continue

        sql = '''
            UPDATE %(db)s.%(file_states)s
            SET file_state = ?
            WHERE file_name = ?
        ''' % self._names

        self._conn.executemany(sql, iter_filenames_states())


class SquirrelStats(Object):
    nfiles = Int.T()
    nnuts = Int.T()
    codes = List.T(List.T(String.T()))
    kinds = List.T(String.T())


class Squirrel(Selection):

    def __init__(self, database=None, persistent=None):
        Selection.__init__(self, database=database, persistent=persistent)
        c = self._conn

        self._names.update({
            'nuts': self.name + '_nuts',
            'kind_codes': self.name + '_kind_codes'})

        c.execute(
            '''CREATE TABLE IF NOT EXISTS %(db)s.%(nuts)s (
                file_id integer,
                file_segment integer,
                file_element integer,
                kind_codes_id integer,
                tmin_seconds integer,
                tmin_offset float,
                tmax_seconds integer,
                tmax_offset float,
                deltat float,
                kscale integer,
                PRIMARY KEY (file_id, file_segment, file_element))
            ''' % self._names)

        c.execute(
            '''CREATE TABLE IF NOT EXISTS %(db)s.%(kind_codes)s (
                kind_codes_id integer PRIMARY KEY,
                count integer)''' % self._names)

        c.execute(
            '''CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_tmin_seconds
                ON %(nuts)s (tmin_seconds)
            ''' % self._names)

        c.execute(
            '''CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_tmax_seconds
                ON %(nuts)s (tmax_seconds)''' % self._names)

        c.execute(
            '''CREATE INDEX IF NOT EXISTS %(db)s.%(nuts)s_index_kscale
                ON %(nuts)s (kscale, tmin_seconds)''' % self._names)

        c.execute(
            '''CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_delete_nuts
                BEFORE DELETE ON main.files FOR EACH ROW
                BEGIN
                  DELETE FROM %(nuts)s where file_id == old.rowid;
                END''' % self._names)

        c.execute(
            '''CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_inc_kind_codes
                BEFORE INSERT ON %(nuts)s FOR EACH ROW
                BEGIN
                    INSERT OR IGNORE INTO %(kind_codes)s VALUES
                    (new.kind_codes_id, 0);
                    UPDATE %(kind_codes)s
                    SET count = count + 1
                    WHERE new.kind_codes_id == %(kind_codes)s.kind_codes_id;
                END''' % self._names)

        c.execute(
            '''CREATE TRIGGER IF NOT EXISTS %(db)s.%(nuts)s_dec_kind_codes
                BEFORE DELETE ON %(nuts)s FOR EACH ROW
                BEGIN
                    UPDATE %(kind_codes)s
                    SET count = count - 1
                    WHERE old.kind_codes_id == %(kind_codes)s.kind_codes_id;
                END''' % self._names)

    def delete(self):
        self._conn.execute(
            'DROP TABLE %(db)s.%(nuts)s' % self._names)

        self._conn.execute(
            'DROP TABLE %(db)s.%(kind_codes)s' % self._names)

        Selection.delete(self)

    def print_tables(self, stream=None):
        if stream is None:
            stream = sys.stdout

        w = stream.write

        w('\n')
        for table in [
                '%(db)s.%(file_states)s',
                '%(db)s.%(nuts)s']:

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

    def add(self, filenames, kinds=None, format='detect', check_mtime=True):
        if isinstance(kinds, str):
            kinds = (kinds,)

        Selection.add(self, filenames)
        self._load(format, check_mtime)
        self._update_nuts(kinds)

    def _load(self, format, check_mtime):
        for _ in io.iload(
                self,
                content=[],
                skip_unchanged=True,
                format=format,
                check_mtime=check_mtime):
            pass

    def _update_nuts(self, kinds):
        c = self._conn
        w_kinds = ''
        args = []
        if kinds is not None:
            w_kinds = 'AND nuts.kind IN (%s)' % ', '.join('?'*len(kinds))
            args.append(kinds)

        c.execute((
            '''INSERT INTO %(db)s.%(nuts)s
                SELECT nuts.* FROM %(db)s.%(file_states)s
                INNER JOIN files
                ON %(db)s.%(file_states)s.file_name = files.file_name
                INNER JOIN nuts
                ON files.rowid = nuts.file_id
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
                files.file_name,
                files.file_format,
                files.file_mtime,
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
            ON files.rowid == %(db)s.%(nuts)s.file_id
            INNER JOIN kind_codes
            ON %(db)s.%(nuts)s.kind_codes_id == kind_codes.rowid
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
                files.file_name,
                files.file_format,
                files.file_mtime,
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
            ON files.rowid == %(db)s.%(nuts)s.file_id
            INNER JOIN kind_codes
            ON %(db)s.%(nuts)s.kind_codes_id == kind_codes.rowid
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

    def iter_codes(self, kind=None):
        args = []
        sel = ''
        if kind is not None:
            sel = 'AND kind_codes.codes == ?'
            args.append(kind)

        sql = ('''
            SELECT DISTINCT kind_codes.codes FROM %(db)s.%(kind_codes)s
            INNER JOIN kind_codes
            WHERE %(db)s.%(kind_codes)s.kind_codes_id == kind_codes.rowid
                AND kind_codes.count > 0
                ''' + sel + '''
            ORDER BY kind_codes.codes
        ''') % self._names

        for row in self._conn.execute(sql, args):
            yield tuple(row[0].split('\0'))

    def iter_kinds(self, codes=None):
        args = []
        sel = ''
        if codes is not None:
            sel = 'AND kind_codes.codes == ?'
            args.append('\0'.join(codes))

        sql = ('''
            SELECT DISTINCT kind_codes.kind FROM %(db)s.%(kind_codes)s
            INNER JOIN kind_codes
            WHERE %(db)s.%(kind_codes)s.kind_codes_id == kind_codes.rowid
                AND kind_codes.count > 0
                ''' + sel + '''
            ORDER BY kind_codes.kind
        ''') % self._names

        for row in self._conn.execute(sql):
            yield row[0]

    def update_channel_inventory(self, selection):
        for source in self._sources:
            source.update_channel_inventory(selection)
            for fn in source.get_channel_filenames(selection):
                self.add(fn)

    def get_nfiles(self):
        sql = '''SELECT COUNT(*) FROM %(db)s.%(file_states)s''' % self._names
        for row in self._conn.execute(sql):
            return row[0]

    def get_nnuts(self):
        sql = '''SELECT COUNT(*) FROM %(db)s.%(nuts)s''' % self._names
        for row in self._conn.execute(sql):
            return row[0]

    def get_stats(self):
        return SquirrelStats(
            nfiles=self.get_nfiles(),
            nnuts=self.get_nnuts(),
            kinds=list(self.iter_kinds()),
            codes=list(self.iter_codes()))

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
            '''CREATE TABLE IF NOT EXISTS files (
                file_name text PRIMARY KEY,
                file_format text,
                file_mtime float)''')

        c.execute(
            '''CREATE TABLE IF NOT EXISTS nuts (
                file_id integer,
                file_segment integer,
                file_element integer,
                kind_codes_id text,
                tmin_seconds integer,
                tmin_offset float,
                tmax_seconds integer,
                tmax_offset float,
                deltat float,
                kscale integer,
                PRIMARY KEY (file_id, file_segment, file_element))''')

        c.execute(
            '''CREATE TABLE IF NOT EXISTS kind_codes (
                kind text,
                codes text,
                count integer,
                PRIMARY KEY (kind, codes))''')

        c.execute(
            '''CREATE INDEX IF NOT EXISTS index_nuts_file_id
                ON nuts (file_id)''')

        c.execute(
            '''CREATE TRIGGER IF NOT EXISTS delete_nuts
                BEFORE DELETE ON files FOR EACH ROW
                BEGIN
                  DELETE FROM nuts where file_id == old.rowid;
                END''')

        c.execute(
            '''CREATE TRIGGER IF NOT EXISTS decrement_kind_codes
                BEFORE DELETE ON nuts FOR EACH ROW
                BEGIN
                    UPDATE kind_codes
                    SET count = count - 1
                    WHERE old.kind_codes_id == rowid;
                END''')

        self._conn.commit()
        c.close()

    def dig(self, nuts):
        if not nuts:
            return

        c = self._conn.cursor()
        by_files = defaultdict(list)
        count_kind_codes = Counter()
        for nut in nuts:
            k = nut.file_name, nut.file_format, nut.file_mtime
            by_files[k].append(nut)
            count_kind_codes[nut.kind, nut.codes] += 1

        c.executemany(
            'INSERT OR IGNORE INTO kind_codes VALUES (?,?,0)',
            [kc for kc in count_kind_codes])

        c.executemany(
            '''
                UPDATE kind_codes
                SET count = count + ?
                WHERE kind == ? AND codes == ?
            ''',
            [(inc, kind, codes) for (kind, codes), inc
             in count_kind_codes.items()])

        for k, file_nuts in iitems(by_files):
            c.execute('DELETE FROM files WHERE file_name = ?', k[0:1])
            c.execute('INSERT INTO files VALUES (?,?,?)', k)
            file_id = c.lastrowid

            c.executemany(
                '''
                    INSERT INTO nuts VALUES
                        (?,?,?, (
                            SELECT rowid FROM kind_codes
                            WHERE kind == ? AND codes == ?
                         ), ?,?,?,?,?,?)
                ''',
                [(file_id, nut.file_segment, nut.file_element,
                  nut.kind, nut.codes,
                  nut.tmin_seconds, nut.tmin_offset,
                  nut.tmax_seconds, nut.tmax_offset,
                  nut.deltat, nut.kscale) for nut in file_nuts])

        self._need_commit = True
        c.close()

    def undig(self, filename):
        sql = '''
            SELECT
                files.file_name,
                files.file_format,
                files.file_mtime,
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
            INNER JOIN nuts ON files.rowid = nuts.file_id
            INNER JOIN kind_codes ON nuts.kind_codes_id == kind_codes.rowid
            WHERE file_name == ?'''

        return [model.Nut(values_nocheck=row)
                for row in self._conn.execute(sql, (filename,))]

    def undig_all(self):
        sql = '''
            SELECT
                files.file_name,
                files.file_format,
                files.file_mtime,
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
            INNER JOIN nuts ON files.rowid == nuts.file_id
            INNER JOIN kind_codes ON nuts.kind_codes_id == kind_codes.rowid
        '''

        nuts = []
        fn = None
        for values in self._conn.execute(sql):
            if fn is not None and values[0] != fn:
                yield fn, nuts
                nuts = []

            if values[1] is not None:
                nuts.append(model.Nut(values_nocheck=values))

            fn = values[0]

        if fn is not None:
            yield fn, nuts

    def undig_many(self, filenames):
        selection = self.new_selection(filenames)

        for fn, nuts in selection.undig_grouped():
            yield fn, nuts

        del selection

    def get_mtime(self, filename):
        sql = '''
            SELECT file_mtime
            FROM files
            WHERE file_name = ?'''

        for row in self._conn.execute(sql, (filename,)):
            return row[0]

        return None

    def get_mtimes(self, filenames):
        selection = self.new_selection(filenames)
        mtimes = selection.get_mtimes()
        del selection
        return mtimes

    def new_selection(self, filenames=None, state=0):
        selection = Selection(self)
        if filenames:
            selection.add(filenames, state=state)
        return selection

    def commit(self):
        if self._need_commit:
            self._conn.commit()
            self._need_commit = False

    def undig_content(self, nut):
        return None

    def remove(self, filename):
        self._conn.execute(
            'DELETE FROM files WHERE file_name = ?', (filename,))


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
