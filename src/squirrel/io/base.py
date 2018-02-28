from __future__ import absolute_import
import logging
from builtins import str as newstr

from pyrocko.io_common import FileLoadError
from ..squirrel import Selection
from .backends import mseed, sac, datacube, stationxml, textfiles, virtual

backend_modules = [mseed, sac, datacube, stationxml, textfiles, virtual]


logger = logging.getLogger('pyrocko.sqirrel.io')


def update_format_providers():
    '''Update global mapping from file format to io backend module.'''

    global g_format_providers
    g_format_providers = {}
    for mod in backend_modules:
        for format in mod.provided_formats():
            if format not in g_format_providers:
                g_format_providers[format] = []

            g_format_providers[format].append(mod)


g_format_providers = {}
update_format_providers()


class FormatDetectionFailed(FileLoadError):
    def __init__(self, file_path):
        FileLoadError.__init__(
            self, 'format detection failed for file: %s' % file_path)


class UnknownFormat(Exception):
    def __init__(self, format):
        FileLoadError.__init__(
            self, 'unknown format: %s' % format)


def get_backend(fmt):
    try:
        return g_format_providers[fmt][0]
    except KeyError:
        raise UnknownFormat(fmt)


def detect_format(file_path):
    '''Determine file type from first 512 bytes.'''

    if file_path.startswith('virtual:'):
        return 'virtual'

    try:
        with open(file_path, 'rb') as f:
            data = f.read(512)

    except OSError as e:
        raise FormatDetectionFailed(file_path)

    fmt = None
    for mod in backend_modules:
        fmt = mod.detect(data)
        if fmt is not None:
            return fmt

    raise FormatDetectionFailed(file_path)


def iload(
        file_paths,
        segment=None,
        format='detect',
        database=None,
        check=True,
        commit=True,
        skip_unchanged=False,
        content=['waveform', 'station', 'channel', 'response', 'event']):

    '''
    Iteratively load content or index from files.

    :param file_paths: iterator yielding file names to load from or
        :py:class:`pyrocko.squirrel.Selection` object
    :param segment: ``str`` file-specific segment identifier (con only be used
        when loading from a single file.
    :param format: ``str`` file format or ``'detect'`` for autodetection
    :param database: :py:class:`pyrocko.squirrel.Database` object to use
        as index cache
    :param check:  ``bool`` flag, if ``True``, investigate modification time
        and file sizes of known files to debunk modified files (pessimistic),
        or ``False`` to deactivate checks (optimistic)
    :param commit: ``bool`` flag, whether to commit updated information to the
        index cache
    :param skip_unchanged: ``bool`` flag, if ``True``, only yield index nuts
        for new / modified files
    :param content: list of strings, selection of content types to load
    '''

    n_db = 0
    n_load = 0
    selection = None

    if isinstance(file_paths, (str, newstr)):
        file_paths = [file_paths]
    else:
        if segment is not None:
            raise TypeError(
                'iload: segment argument can only be used when loading from '
                'a single file')

        if isinstance(file_paths, Selection):
            selection = file_paths
            if database is not None:
                raise TypeError(
                    'iload: database argument must be None when called with a '
                    'selection')

            database = selection.get_database()

    temp_selection = None
    if database:
        if not selection:
            temp_selection = database.new_selection(file_paths, state=1)
            selection = temp_selection

        if skip_unchanged:
            selection.flag_unchanged(check)
            it = selection.undig_grouped(skip_unchanged=True)
        else:
            it = selection.undig_grouped()

    else:
        if skip_unchanged:
            raise TypeError(
                'iload: skip_unchanged argument requires database')

        it = ((file_path, []) for file_path in file_paths)

    n_files = 0
    for file_path, old_nuts in it:
        n_files += 1
        if database and commit and n_files % 1000 == 0:
            database.commit()

        try:
            if check and old_nuts and old_nuts[0].file_modified():
                old_nuts = []

            if segment is not None:
                old_nuts = [nut for nut in old_nuts if nut.segment == segment]

            if old_nuts:
                db_only_operation = not content or all(
                    nut.kind in content and nut.content_in_db
                    for nut in old_nuts)

                if db_only_operation:
                    logger.debug('using cached information for file %s, '
                                 % file_path)

                    for nut in old_nuts:
                        if nut.kind in content:
                            database.undig_content(nut)

                        n_db += 1
                        yield nut

                    continue

            if format == 'detect':
                if old_nuts and not old_nuts[0].file_modified():
                    format_this = old_nuts[0].file_format
                else:
                    format_this = detect_format(file_path)
            else:
                format_this = format

            mod = get_backend(format_this)
            mtime, size = mod.get_stats(file_path)

            logger.debug('reading file %s' % file_path)
            nuts = []
            for nut in mod.iload(format_this, file_path, segment, content):
                nut.file_path = file_path
                nut.file_format = format_this
                nut.file_mtime = mtime
                nut.file_size = size

                nuts.append(nut)
                n_load += 1
                yield nut

            if database and nuts != old_nuts:
                if segment is not None:
                    nuts = mod.iload(format_this, file_path, None, [])
                    for nut in nuts:
                        nut.file_path = file_path
                        nut.file_format = format_this
                        nut.file_mtime = mtime

                database.dig(nuts)

        except FileLoadError:
            logger.error('an error occured while reading file: %s' % file_path)
            if database:
                database.remove(file_path)

    if database:
        if commit:
            database.commit()

        if temp_selection:
            del temp_selection

    logger.debug('iload: from db: %i, from files: %i, files: %i' % (
        n_db, n_load, n_files))
