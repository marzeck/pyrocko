import time
import os
import copy
import hashlib
import logging
try:
    import cPickle as pickle
except ImportError:
    import pickle
import os.path as op
from .base import Source, Selection
from pyrocko.client import fdsn

from pyrocko import config, util

fdsn.g_timeout = 60.

logger = logging.getLogger('pyrocko.squirrel.client.fdsn')

sites_not_supporting_startbefore = ['geonet']


def diff(fn_a, fn_b):
    try:
        if os.stat(fn_a)[8] != os.stat(fn_b)[8]:
            return True

    except OSError:
        return True

    with open(fn_a, 'rb') as fa:
        with open(fn_b, 'rb') as fb:
            while True:
                a = fa.read(1024)
                b = fb.read(1024)
                if a != b:
                    return True

                if len(a) == 0 or len(b) == 0:
                    return False


def ehash(s):
    return hashlib.sha1(s.encode('utf8')).hexdigest()


class FDSNSource(Source):

    def __init__(
            self, site,
            user_credentials=None, auth_token=None,
            noquery_age_max=3600.,
            cache_dir=None):

        Source.__init__(self)

        self._site = site
        self._selection = None
        self._noquery_age_max = noquery_age_max

        s = site
        if auth_token:
            s += auth_token
        if user_credentials:
            s += user_credentials[0]
            s += user_credentials[1]

        self._auth_token = auth_token
        self._user_credentials = user_credentials

        self._cache_dir = op.join(
            cache_dir or config.config().cache_dir,
            'fdsn',
            ehash(s))

        util.ensuredir(self._cache_dir)
        self._load_selection()

    def get_channel_file_paths(self, selection=None):
        return [op.join(self._cache_dir, 'channels.stationxml')]

    def update_channel_inventory(self, selection=None):
        if selection is None:
            selection = Selection()

        if self._selection and self._selection.contains(selection) \
                and not self._stale_channel_inventory(selection):

            logger.info(
                'using cached channel information for site %s'
                % self._site)

            return

        if self._selection:
            selection = copy.deepcopy(self._selection)
            selection.add(selection)

        channel_sx = self._do_channel_query(selection)
        channel_sx.created = None  # timestamp would ruin diff

        fn = self.get_channel_file_paths(selection)[0]
        fn_temp = fn + '.%i.temp' % os.getpid()
        channel_sx.dump_xml(filename=fn_temp)

        if diff(fn, fn_temp):
            os.rename(fn_temp, fn)
            logger.info('changed: %s' % fn)
        else:
            logger.info('no change: %s' % fn)
            os.unlink(fn_temp)

        self._selection = selection
        self._dump_selection()

    def _do_channel_query(self, selection):
        extra_args = {
            'iris': dict(matchtimeseries=True),
        }.get(self._site, {})

        if self._site in sites_not_supporting_startbefore:
            if selection.tmin is not None:
                extra_args['starttime'] = selection.tmin
            if selection.tmax is not None:
                extra_args['endtime'] = selection.tmax

        else:
            if selection.tmin is not None:
                extra_args['endafter'] = selection.tmin
            if selection.tmax is not None:
                extra_args['startbefore'] = selection.tmax

        extra_args.update(
            includerestricted=(
                self._user_credentials is not None
                or self._auth_token is not None))

        logger.info(
            'querying channel information from site %s'
            % self._site)

        channel_sx = fdsn.station(
            site=self._site,
            format='text',
            level='channel',
            **extra_args)

        return channel_sx

    def _get_selection_file_path(self):
        return op.join(self._cache_dir, 'selection.pickle')

    def _load_selection(self):
        fn = self._get_selection_file_path()
        if op.exists(fn):
            with open(fn, 'rb') as f:
                self._selection = pickle.load(f)
        else:
            self._selection = None

    def _dump_selection(self):
        with open(self._get_selection_file_path(), 'wb') as f:
            pickle.dump(self._selection, f)

    def _stale_channel_inventory(self, selection):
        for file_path in self.get_channel_file_paths(selection):
            try:
                t = os.stat(file_path)[8]
                return t < time.time() - self._noquery_age_max
            except OSError:
                return True

        return False
