from __future__ import print_function, absolute_import

import os
import unittest
import tempfile
import shutil
import os.path as op

import numpy as num

from . import common
from pyrocko import squirrel, util, pile, io, trace


class SquirrelTestCase(unittest.TestCase):

    test_files = [
        ('test1.mseed', 'mseed'),
        ('test2.mseed', 'mseed'),
        ('test1.sac', 'sac'),
        ('test1.stationxml', 'stationxml'),
        ('test2.stationxml', 'stationxml'),
        ('test1.stations', 'pyrocko_stations'),
        ('test1.cube', 'datacube')]

    def test_detect(self):
        for (fn, format) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            self.assertEqual(format, squirrel.detect_format(fpath))

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

    def test_query_mtimes(self):
        fpaths = [
            common.test_data_file(fn)
            for (fn, _) in SquirrelTestCase.test_files]

        database = squirrel.Database()
        for nut in squirrel.iload(fpaths, database=database, content=[]):
            pass

        mtimes_ref = dict(
            (fpath, os.stat(fpath)[8]) for fpath in fpaths)

        def check(fpaths, mtimes):
            for fpath, mtime in zip(fpaths, mtimes):
                self.assertEqual(mtimes_ref.get(fpath, None), mtime)

        fpaths1 = fpaths + ['nonexistent']
        mtimes = database.get_mtimes(fpaths)
        check(fpaths, mtimes)

        fpaths2 = fpaths1[::-2]
        mtimes2 = database.get_mtimes(fpaths2)
        check(fpaths2, mtimes2)

        mtimes3 = [database.get_mtime(fpath) for fpath in fpaths1]
        check(fpaths1, mtimes3)

    def test_dig_undig(self):
        nuts = []
        for file_name in 'abcde':
            for file_element in range(2):
                nuts.append(squirrel.Nut(
                    file_name=file_name,
                    file_format='test',
                    file_mtime=0.0,
                    file_segment=0,
                    file_element=file_element,
                    kind='test'))

        database = squirrel.Database()
        database.dig(nuts)

        data = []
        for file_name in 'abcde':
            nuts2 = database.undig(file_name)
            for nut in nuts2:
                data.append((nut.file_name, nut.file_element))

        self.assertEqual(
            [(file_name, i) for file_name in 'abcde' for i in range(2)],
            data)

        data = []
        for fn, nuts2 in database.undig_many(filenames=['a', 'c']):
            for nut in nuts2:
                data.append((nut.file_name, nut.file_element))

        self.assertEqual(
            [(file_name, i) for file_name in 'ac' for i in range(2)],
            data)

        data = []
        for fn, nuts3 in database.undig_all():
            for nut in nuts3:
                data.append((nut.file_name, nut.file_element))

        self.assertEqual(
            [(file_name, i) for file_name in 'abcde' for i in range(2)],
            data)

    def test_add_update(self):

        tempdir = tempfile.mkdtemp('test_add_update')

        def make_files(vers):
            tr = trace.Trace(ydata=num.array([vers], dtype=num.int32))
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

        

        print(sq)
        shutil.rmtree(tempdir)

    def benchmark_chop(self):
        bench = self.test_chop(10000, ne=10)
        print(bench)

    def test_chop(self, nt=100, ne=10):

        tmin_g = util.stt('2000-01-01 00:00:00')
        tmax_g = util.stt('2020-01-01 00:00:00')

        txs = num.sort(num.random.uniform(tmin_g, tmax_g, nt+1))

        all_nuts = []
        for it in range(nt):
            file_name = 'virtual:file_%i' % it
            tmin = txs[it]
            tmax = txs[it+1]
            tmin_seconds, tmin_offset = squirrel.model.tsplit(tmin)
            tmax_seconds, tmax_offset = squirrel.model.tsplit(tmax)
            for file_element in range(ne):
                all_nuts.append(squirrel.Nut(
                    file_name=file_name,
                    file_format='virtual',
                    file_segment=0,
                    file_element=file_element,
                    codes='c%02i' % file_element,
                    tmin_seconds=tmin_seconds,
                    tmin_offset=tmin_offset,
                    tmax_seconds=tmax_seconds,
                    tmax_offset=tmax_offset,
                    kind='test'))

        squirrel.io.virtual.add_nuts(all_nuts)

        dbfilename = '/tmp/squirrel_benchmark_chop.db'
        if os.path.exists(dbfilename):
            os.unlink(dbfilename)

        filldb = not os.path.exists(dbfilename)

        database = squirrel.Database(dbfilename)

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
                check_mtime=False)

        with bench.run('get tspan'):
            tmin, tmax = sq.tspan()

        with bench.run('get codes'):
            for kind, codes in sq.iter_codes():
                pass

        with bench.run('undig span naiv'):
            t = tmin_g
            tinc = 3600*24
            while t < tmax:
                t += tinc
                tmin = t
                tmax = t + tinc

                sq.undig_span_naiv(tmin, tmax)
                break

        with bench.run('undig span'):
            t = tmin_g
            tinc = 3600*24
            while t < tmax:
                t += tinc
                tmin = t
                tmax = t + tinc

                sq.undig_span(tmin, tmax)
                break

        return bench

    def benchmark_loading(self):
        bench = self.test_loading(hours=24)
        print(bench)

    def test_loading(self, with_pile=False, hours=1):
        dir = '/tmp/testdataset_d_%i' % hours
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

        dbfilename = '/tmp/squirrel.db'
        if os.path.exists(dbfilename):
            os.unlink(dbfilename)
        database = squirrel.Database(dbfilename)

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
                                      check_mtime=False):
                ii += 1

            assert ii == len(fns)

        with bench.run('iload, rescan, skip unchanged'):
            ii = 0
            for nut in squirrel.iload(fns, content=[], database=database,
                                      skip_unchanged=True, check_mtime=True):
                ii += 1

            assert ii == 0

        with bench.run('iload, rescan, skip known'):
            ii = 0
            for nut in squirrel.iload(fns, content=[], database=database,
                                      skip_unchanged=True, check_mtime=False):
                ii += 1

            assert ii == 0

        with bench.run('undig'):
            ii = 0
            for fn, nuts in database.undig_many(fns):
                ii += 1

            assert ii == len(fns)

        with bench.run('mtime, file-by-file'):
            for fn in fns:
                database.get_mtime(fn)

        with bench.run('mtime, batch'):
            database.get_mtimes(fns)

        return bench

    def test_source(self):

        tmin = util.str_to_time('2018-01-01 00:00:00')
        tmax = util.str_to_time('2018-01-02 00:00:00')
        database = squirrel.Database()
        sq = squirrel.Squirrel(database=database)
        sq.add_fdsn_site('geofon')
        for s in sq.stations():
            print(s)


if __name__ == "__main__":
    util.setup_logging('test_catalog', 'info')
    unittest.main()
