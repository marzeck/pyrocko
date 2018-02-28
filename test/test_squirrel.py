from __future__ import print_function, absolute_import

import time
import os
import unittest
import tempfile
import shutil
import os.path as op
from collections import defaultdict

import numpy as num

from . import common
from pyrocko import squirrel, util, pile, io, trace
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class SquirrelTestCase(unittest.TestCase):
    tempdirs = []

    test_files = [
        ('test1.mseed', 'mseed'),
        ('test2.mseed', 'mseed'),
        ('test1.sac', 'sac'),
        ('test1.stationxml', 'stationxml'),
        ('test2.stationxml', 'stationxml'),
        ('test1.stations', 'pyrocko_stations'),
        ('test1.cube', 'datacube')]

    @classmethod
    def setUpClass(cls):
        cls.tempdirs.append(tempfile.mkdtemp())
        cls.tempdir = cls.tempdirs[0]

    @classmethod
    def tearDownClass(cls):
        for d in cls.tempdirs:
            shutil.rmtree(d)

    def test_detect(self):
        for (fn, format) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            self.assertEqual(format, squirrel.detect_format(fpath))

        fpath = op.join(self.tempdir, 'emptyfile')
        with open(fpath, 'wb'):
            pass

        with self.assertRaises(squirrel.io.FormatDetectionFailed):
            squirrel.detect_format(fpath)

        with self.assertRaises(squirrel.io.FormatDetectionFailed):
            squirrel.detect_format('nonexist')

    def test_load(self):
        ii = 0
        for (fn, format) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            for nut in squirrel.iload(fpath, content=[]):
                ii += 1

        assert ii == 396

        ii = 0
        database = squirrel.Database()
        for (fn, _) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            for nut in squirrel.iload(fpath, content=[], database=database):
                ii += 1

        assert ii == 396

        ii = 0
        for (fn, _) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            for nut in squirrel.iload(fpath, content=[], database=database):
                ii += 1

        ii = 0
        for (fn, _) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            for nut in squirrel.iload(fpath, database=database):
                ii += 1

        assert ii == 396

        fpaths = [
            common.test_data_file(fn)
            for (fn, _) in SquirrelTestCase.test_files]

        ii = 0
        for nut in squirrel.iload(fpaths, content=[], database=database):
            ii += 1

        assert ii == 396

        fpath = op.join(self.tempdir, 'emptyfile')
        with open(fpath, 'wb'):
            pass

        ii = 0
        for nut in squirrel.iload(fpath):
            ii += 1

        assert ii == 0

        with self.assertRaises(squirrel.io.UnknownFormat):
            for nut in squirrel.iload(fpath, format='nonexist'):
                pass

    def test_squirrel(self):
        db_path = os.path.join(self.tempdir, 'test.squirrel')
        for kinds in [None, 'waveform', ['station', 'channel']]:
            for persistent in [None, 'my_selection1', 'my_selection2']:
                sq = squirrel.Squirrel(database=db_path, persistent=persistent)
                for (fn, format) in SquirrelTestCase.test_files:
                    fpath = common.test_data_file(fn)
                    sq.add(fpath, kinds=kinds)

                s = sq.get_stats()
                if kinds is not None:
                    if isinstance(kinds, str):
                        kinds_ = [kinds]
                    else:
                        kinds_ = kinds

                    for k in sq.get_kinds():
                        assert k in kinds_

                all_codes = set()
                for k in sq.get_kinds():
                    for codes in sq.get_codes(k):
                        all_codes.add(codes)

                assert all_codes == set(sq.get_codes())

                all_kinds = set()
                for c in sq.get_codes():
                    for kind in sq.get_kinds(c):
                        all_kinds.add(kind)

                assert all_kinds == set(sq.get_kinds())

                counts = sq.get_counts()
                for k in counts:
                    assert set(counts[k]) == set(sq.get_counts(k))
                    for (_, codes), count in sq.iter_counts(k):
                        assert count == counts[k][codes]


                db = sq.get_database()
                counts = db.get_counts()
                for k in counts:
                    assert set(counts[k]) == set(db.get_counts(k))


                if persistent is not None:
                    sq.delete()
                else:
                    del sq


    def test_dig_undig(self):
        nuts = []
        for path in 'abcde':
            for file_element in range(2):
                nuts.append(squirrel.Nut(
                    file_path=path,
                    file_format='test',
                    file_segment=0,
                    file_element=file_element,
                    kind='test'))

        database = squirrel.Database()
        database.dig(nuts)

        data = defaultdict(list)
        for path in 'abcde':
            nuts2 = database.undig(path)
            for nut in nuts2:
                data[nut.file_path].append(nut.file_element)

        for path in 'abcde':
            self.assertEqual([0, 1], sorted(data[path]))

        data = defaultdict(list)
        for path in 'ac':
            nuts2 = database.undig(path)
            for nut in nuts2:
                data[nut.file_path].append(nut.file_element)

        for path in 'ac':
            self.assertEqual([0, 1], sorted(data[path]))

        data = defaultdict(list)
        for fn, nuts3 in database.undig_all():
            for nut in nuts3:
                data[nut.file_path].append(nut.file_element)

        for path in 'abcde':
            self.assertEqual([0, 1], sorted(data[path]))

    def test_add_update(self):

        tempdir = os.path.join(self.tempdir, 'test_add_update')

        def make_files(vers):
            tr = trace.Trace(
                tmin=float(vers),
                deltat=1.0,
                ydata=num.array([vers, vers], dtype=num.int32))

            return io.save(tr, op.join(tempdir, 'traces.mseed'))

        database = squirrel.Database()
        sq = squirrel.Squirrel(database)

        assert sq.get_nfiles() == 0
        assert sq.get_nnuts() == 0

        fns = make_files(0)
        sq.add(fns)
        assert sq.get_nfiles() == 1
        assert sq.get_nnuts() == 1
        sq.add(fns)
        assert sq.get_nfiles() == 1
        assert sq.get_nnuts() == 1

        assert sq.time_span() == (0., 1.)

        f = StringIO()
        sq.print_tables(stream=f)
        database.print_tables(stream=f)

        time.sleep(2)

        fns = make_files(1)
        sq.add(fns, check=False)
        assert sq.get_nfiles() == 1
        assert sq.get_nnuts() == 1

        assert list(sq.iter_codes()) == [('', '', 'STA', '', '', '')]
        assert list(sq.iter_kinds()) == ['waveform']

        assert len(list(sq.undig_span(-10., 10.))) == 1
        assert len(list(sq.undig_span(-1., 0.))) == 0
        assert len(list(sq.undig_span(0., 1.))) == 1
        assert len(list(sq.undig_span(1., 2.))) == 0
        assert len(list(sq.undig_span(-1., 0.5))) == 1
        assert len(list(sq.undig_span(0.5, 1.5))) == 1
        assert len(list(sq.undig_span(0.2, 0.7))) == 1

        sq.add(fns, check=True)
        assert sq.get_nfiles() == 1
        assert sq.get_nnuts() == 1

        assert list(sq.iter_codes()) == [('', '', 'STA', '', '', '')]
        assert list(sq.iter_kinds()) == ['waveform']

        assert len(list(sq.undig_span(-10., 10.))) == 1

        shutil.rmtree(tempdir)

        sq.add(fns, check=True)
        assert sq.get_nfiles() == 1
        assert sq.get_nnuts() == 0

        assert list(sq.iter_codes()) == []
        assert list(sq.iter_kinds()) == []

        assert len(list(sq.undig_span(-10., 10.))) == 0

        fns = make_files(2)
        sq.add(fns)
        assert sq.get_nfiles() == 1
        assert sq.get_nnuts() == 1
        sq.remove(fns)
        assert sq.get_nfiles() == 0
        assert sq.get_nnuts() == 0

    def benchmark_chop(self):
        bench = self.test_chop(100000, ne=10)
        print(bench)

    def test_chop(self, nt=100, ne=10):

        tmin_g = util.stt('2000-01-01 00:00:00')
        tmax_g = util.stt('2020-01-01 00:00:00')

        txs = num.sort(num.random.uniform(tmin_g, tmax_g, nt+1))
        txs[0] = tmin_g
        txs[-1] = tmax_g

        all_nuts = []
        for it in range(nt):
            path = 'virtual:file_%i' % it
            tmin = txs[it]
            tmax = txs[it+1]
            tmin_seconds, tmin_offset = squirrel.model.tsplit(tmin)
            tmax_seconds, tmax_offset = squirrel.model.tsplit(tmax)
            for file_element in range(ne):
                all_nuts.append(squirrel.Nut(
                    file_path=path,
                    file_format='virtual',
                    file_segment=0,
                    file_element=file_element,
                    codes='c%02i' % file_element,
                    tmin_seconds=tmin_seconds,
                    tmin_offset=tmin_offset,
                    tmax_seconds=tmax_seconds,
                    tmax_offset=tmax_offset,
                    kind='test'))

        squirrel.io.backends.virtual.add_nuts(all_nuts)

        db_file_path = os.path.join(self.tempdir, 'squirrel_benchmark_chop.db')
        if os.path.exists(db_file_path):
            os.unlink(db_file_path)

        filldb = not os.path.exists(db_file_path)

        database = squirrel.Database(db_file_path)

        bench = common.Benchmark('test_chop (%i x %i)' % (nt, ne))

        if filldb:
            with bench.run('init db'):
                database.dig(all_nuts)
                database.commit()

        with bench.run('undig all'):
            it = 0
            for fn, nuts in database.undig_all():
                it += 1

            assert it == nt

        with bench.run('add to squirrel'):
            sq = squirrel.Squirrel(database=database)
            sq.add(
                ('virtual:file_%i' % it for it in range(nt)),
                check=False)

        with bench.run('get time span'):
            tmin, tmax = sq.time_span()

        with bench.run('get codes'):
            for codes in sq.iter_codes():
                pass

        with bench.run('get total size'):
            sq.get_total_size()

        expect = []
        nwin = 100
        tinc = 24 * 3600.

        with bench.run('undig span naiv'):
            for iwin in range(nwin):
                tmin = tmin_g + iwin * tinc
                tmax = tmin_g + (iwin+1) * tinc

                expect.append(len(list(sq.undig_span_naiv(tmin, tmax))))
                assert expect[-1] >= 10

        with bench.run('undig span'):
            for iwin in range(nwin):
                tmin = tmin_g + iwin * tinc
                tmax = tmin_g + (iwin+1) * tinc

                assert len(list(sq.undig_span(tmin, tmax))) == expect[iwin]

        return bench

    def benchmark_loading(self):
        bench = self.test_loading(hours=24)
        print(bench)

    def test_loading(self, with_pile=False, hours=1):
        dir = op.join(tempfile.gettempdir(), 'testdataset_d_%i' % hours)

        if not os.path.exists(dir):
            common.make_dataset(dir, tinc=36., tlen=hours*common.H)

        fns = sorted(util.select_files([dir], show_progress=False))

        bench = common.Benchmark('test_load')
        if with_pile:
            cachedirname = tempfile.mkdtemp('testcache')

            with bench.run('pile, initial scan'):
                pile.make_pile(
                    fns, fileformat='detect', cachedirname=cachedirname,
                    show_progress=False)

            with bench.run('pile, rescan'):
                pile.make_pile(
                    fns, fileformat='detect', cachedirname=cachedirname,
                    show_progress=False)

            shutil.rmtree(cachedirname)

        with bench.run('plain load baseline'):
            ii = 0
            for fn in fns:
                for tr in io.load(fn, getdata=False):
                    ii += 1

        with bench.run('iload, without db'):
            ii = 0
            for nut in squirrel.iload(fns, content=[]):
                ii += 1

            assert ii == len(fns)

        db_file_path = op.join(self.tempdir, 'db.squirrel')
        if os.path.exists(db_file_path):
            os.unlink(db_file_path)
        database = squirrel.Database(db_file_path)

        with bench.run('iload, with db'):
            ii = 0
            for nut in squirrel.iload(fns, content=[], database=database):
                ii += 1

            assert ii == len(fns)

        with bench.run('iload, rescan'):
            ii = 0
            for nut in squirrel.iload(fns, content=[], database=database):
                ii += 1

            assert ii == len(fns)

        with bench.run('iload, rescan, no mtime check'):
            ii = 0
            for nut in squirrel.iload(fns, content=[], database=database,
                                      check=False):
                ii += 1

            assert ii == len(fns)

        with bench.run('iload, rescan, skip unchanged'):
            ii = 0
            for nut in squirrel.iload(fns, content=[], database=database,
                                      skip_unchanged=True, check=True):
                ii += 1

            assert ii == 0

        with bench.run('iload, rescan, skip known'):
            ii = 0
            for nut in squirrel.iload(fns, content=[], database=database,
                                      skip_unchanged=True, check=False):
                ii += 1

            assert ii == 0

        sel = database.new_selection(fns, state=1)

        with bench.run('iload, rescan, skip known, preselected'):
            ii = 0
            for nut in squirrel.iload(sel, content=[],
                                      skip_unchanged=True, check=False):
                ii += 1

            assert ii == 0

        del sel

        with bench.run('undig'):
            ii = 0
            for fn, nuts in database.undig_many(fns):
                ii += 1

            assert ii == len(fns)

        with bench.run('stats'):
            s = database.get_stats()
            assert s.nfiles == len(fns)
            assert s.nnuts == len(fns)
            assert s.kinds == ['waveform']
            for kind in s.kinds:
                for codes in s.codes:
                    assert s.counts[kind][codes] == len(fns) // 30

        with bench.run('add to squirrel'):
            sq = squirrel.Squirrel(database=database)
            sq.add(fns)

        with bench.run('stats'):
            s = sq.get_stats()
            assert s.nfiles == len(fns)
            assert s.nnuts == len(fns)
            assert s.kinds == ['waveform']
            for kind in s.kinds:
                for codes in s.codes:
                    assert s.counts[kind][codes] == len(fns) // 30

        return bench

    def test_source(self):
        tmin = util.str_to_time('2018-01-01 00:00:00')
        tmax = util.str_to_time('2018-01-02 00:00:00')
        database = squirrel.Database()
        sq = squirrel.Squirrel(database=database)
        sq.add_fdsn_site('geofon')
        sel = squirrel.client.Selection(tmin=tmin, tmax=tmax)
        sq.update_channel_inventory(sel)


if __name__ == "__main__":
    util.setup_logging('test_catalog', 'info')
    unittest.main()
