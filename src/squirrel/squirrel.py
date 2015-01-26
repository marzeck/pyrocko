import os
import threading
import sqlite3
from collections import defaultdict, Counter

from pyrocko.squirrel import model, io
from pyrocko.squirrel.client import fdsn


def iitems(d):
    try:
        return d.iteritems()
    except AttributeError:
        return d.items()


icount = 0
lock = threading.Lock()


def make_unique_name():
    with lock:
        global icount
        name = '%i_%i' % (os.getpid(), icount)
        icount += 1

    return name


class Selection(object):
    def __init__(self, squirrel):
        self.name = 'selection_' + make_unique_name()
        self.name_nuts = self.name + '_nuts'
        self._squirrel = squirrel
        self._conn = self._squirrel.get_connection()

        self._conn.execute(
            'CREATE TEMP TABLE %s (file_name text)' % self.name)

        self._squirrel.selections.append(self)

    def add(self, filenames):
        self._conn.executemany(
            'INSERT INTO temp.%s VALUES (?)' % self.name,
            ((s,) for s in filenames))

    def delete(self):
        self._conn.execute(
            'DROP TABLE temp.%s' % self.name)

        self._squirrel.selections.remove(self)

    def undig(self):

        sql = '''
            SELECT
                temp.%s.file_name,
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
            FROM temp.%s
            LEFT OUTER JOIN files ON temp.%s.file_name = files.file_name
            LEFT OUTER JOIN nuts ON files.rowid = nuts.file_id
            LEFT OUTER JOIN kind_codes ON nuts.kind_codes_id == kind_codes.rowid 
            ORDER BY temp.%s.rowid
        ''' % (self.name, self.name, self.name, self.name)

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
            SELECT files.file_name, files.file_mtime
            FROM temp.%s
            LEFT OUTER JOIN files ON temp.%s.file_name = files.file_name
            ORDER BY temp.%s.rowid
        ''' % (self.name, self.name, self.name)

        for row in self._conn.execute(sql):
            yield row

    def get_mtimes(self):
        return list(mtime for (_, mtime) in self.iter_mtimes())

    def filter_modified_or_new(self, check_mtime=True):

        def iter_filtered():
            for filename, mtime_db in self.iter_mtimes():
                if mtime_db is None or not os.path.exists(filename):
                    yield filename

                if check_mtime:
                    try:
                        mtime_file = os.stat(filename)[8]
                    except OSError:
                        yield filename
                        continue

                    if mtime_db != mtime_file:
                        yield filename

        filtered_selection = Selection(self._squirrel)
        filtered_selection.add(iter_filtered())
        return filtered_selection

    def __len__(self):
        sql = '''SELECT COUNT(*) FROM temp.%s''' % self.name
        for row in self._conn.execute(sql):
            return row[0]

    def __str__(self):
        return '''
squirrel selection "%s"
    files: %i''' % (self.name, len(self))


class Squirrel(object):
    def __init__(self, database=':memory:'):
        self._conn = sqlite3.connect(database)
        self._conn.text_factory = str
        self._initialize_db()
        self._need_commit = False
        self._sources = []
        self.selections = []
        self.global_selection = self.new_selection()

    def __del__(self):
        self.global_selection.delete()

    def get_connection(self):
        return self._conn

    def add(self, filenames):
        def iload():
            file_name = None
            for nut in io.iload(filenames, squirrel=self):
                if nut.file_name != file_name:
                    file_name = nut.file_name
                    yield file_name

        self.global_selection.add(iload())

    def add_fdsn_site(self, site):
        self._sources.append(fdsn.FDSNSource(site))

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
            '''CREATE INDEX IF NOT EXISTS index_nuts_tmin_seconds
                ON nuts (tmin_seconds)''')

        c.execute(
            '''CREATE INDEX IF NOT EXISTS index_nuts_tmax_seconds
                ON nuts (tmax_seconds)''')

        c.execute(
            '''CREATE INDEX IF NOT EXISTS index_nuts_kscale_tmin_seconds
                ON nuts (kscale, tmin_seconds)''')

        c.execute(
            '''CREATE TRIGGER IF NOT EXISTS delete_nuts
                BEFORE DELETE ON files FOR EACH ROW
                BEGIN
                  DELETE FROM nuts where file_id = old.rowid;
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

        for fn, nuts in selection.undig():
            yield fn, nuts

        selection.delete()

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
        selection.delete()
        return mtimes

    def new_selection(self, filenames=None):
        selection = Selection(self)
        if filenames:
            selection.add(filenames)
        return selection

    def choose(self, filenames):
        self._conn.execute(
            'CREATE TEMP TABLE choosen_files (file_name text)')

        self._conn.executemany(
            'INSERT INTO temp.choosen_files VALUES (?)',
            ((s,) for s in filenames))

        self._conn.execute(
            '''CREATE TEMP TABLE choosen_nuts (
                file_id integer,
                file_segment integer,
                file_element integer,
                kind text,
                codes text,
                tmin_seconds integer,
                tmin_offset float,
                tmax_seconds integer,
                tmax_offset float,
                deltat float,
                kscale integer,
                PRIMARY KEY (file_id, file_segment, file_element))''')

        sql = '''INSERT INTO temp.choosen_nuts
            SELECT nuts.* FROM temp.choosen_files
            INNER JOIN files ON temp.choosen_files.file_name = files.file_name
            INNER JOIN nuts ON files.rowid = nuts.file_id
        '''

        self._conn.execute(sql)

        self._conn.execute(
            'DROP TABLE temp.choosen_files')

        self._conn.execute(
            'DROP TABLE temp.choosen_nuts')

    def commit(self):
        if self._need_commit:
            self._conn.commit()
            self._need_commit = False

    def undig_content(self, nut):
        return None

    def undig_span_naiv(self, tmin, tmax):
        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

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
            WHERE nuts.tmax_seconds >= ? AND nuts.tmin_seconds <= ?
        '''

        nuts = []
        for row in self._conn.execute(sql, (tmin_seconds, tmax_seconds+1)):
            nuts.append(model.Nut(values_nocheck=row))

        return nuts

    def undig_span(self, tmin, tmax):
        tmin_seconds, tmin_offset = model.tsplit(tmin)
        tmax_seconds, tmax_offset = model.tsplit(tmax)

        tscale_edges = model.tscale_edges

        tmin_cond = []
        args = []
        for kscale in range(len(tscale_edges) + 1):
            if kscale != len(tscale_edges):
                tscale = tscale_edges[kscale]
                tmin_cond.append('''
                    (nuts.kscale == ? AND nuts.tmin_seconds BETWEEN ? AND ?)
                ''')
                args.extend(
                    (kscale, tmax_seconds - tscale - 1, tmax_seconds + 1))

            else:
                tmin_cond.append('''
                    (nuts.kscale == ? AND nuts.tmin_seconds <= ?)
                ''')

                args.extend(
                    (kscale, tmax_seconds + 1))

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
            WHERE ( ''' + ' OR '.join(tmin_cond) + ''')
                AND nuts.tmax_seconds >= ?
        '''
        print(sql)
        args.append(tmin_seconds)

        nuts = []
        for row in self._conn.execute(sql, args):
            nuts.append(model.Nut(values_nocheck=row))

        return nuts

    def tspan(self):
        sql = '''SELECT MIN(nuts.tmin_seconds) FROM nuts'''
        tmin = None
        for row in self._conn.execute(sql):
            tmin = row[0]

        tmax = None
        sql = '''SELECT MAX(nuts.tmax_seconds) FROM nuts'''
        for row in self._conn.execute(sql):
            tmax = row[0]

        return tmin, tmax

    def iter_codes(self, kind=None):
        sql = '''
            SELECT kind, codes from kind_codes
        '''
        for row in self._conn.execute(sql):
            yield row[0], row[1].split('\0')

    def update_channel_inventory(self, selection):
        for source in self._sources:
            source.update_channel_inventory(selection)
            for fn in source.get_channel_filenames(selection):
                self.add(fn)

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
