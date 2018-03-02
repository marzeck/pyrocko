from __future__ import absolute_import
import logging
from builtins import str as newstr

from pyrocko.io_common import FileLoadError
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
    '''
    Exception raised when file format detection fails.
    '''

    def __init__(self, path):
        FileLoadError.__init__(
            self, 'format detection failed for file: %s' % path)


class UnknownFormat(Exception):
    '''
    Exception raised when user requests an unknown file format.
    '''

    def __init__(self, format):
        FileLoadError.__init__(
            self, 'unknown format: %s' % format)


def get_backend(fmt):
    '''
    Get squirrel io backend module for a given file format.

    :params str fmt: format identifier
    '''

    try:
        return g_format_providers[fmt][0]
    except KeyError:
        raise UnknownFormat(fmt)


def detect_format(path):
    '''
    Determine file type from first 512 bytes.

    :param str path: path of file
    '''

    if path.startswith('virtual:'):
        return 'virtual'

    try:
        with open(path, 'rb') as f:
            data = f.read(512)

    except OSError as e:
        raise FormatDetectionFailed(path)

    fmt = None
    for mod in backend_modules:
        fmt = mod.detect(data)
        if fmt is not None:
            return fmt

    raise FormatDetectionFailed(path)


def iload(
        paths,
        segment=None,
        format='detect',
        database=None,
        check=True,
        commit=True,
        skip_unchanged=False,
        content=['waveform', 'station', 'channel', 'response', 'event']):

    '''
    Iteratively load content or index/reindex meta-information from files.

    :param paths: iterator yielding file names to load from or
        :py:class:`pyrocko.squirrel.Selection` object
    :param str segment: file-specific segment identifier (con only be used
        when loading from a single file.
    :param str format: file format identifier or ``'detect'`` for autodetection
    :param database: database to use for meta-information caching
    :type database: :py:class:`pyrocko.squirrel.Database`
    :param bool check: if ``True``, investigate modification time and file
        sizes of known files to debunk modified files (pessimistic mode), or
        ``False`` to deactivate checks (optimistic mode)
    :param bool commit: flag, whether to commit updated information to the
        meta-information database
    :param bool skip_unchanged: if ``True``, only yield index nuts
        for new / modified files
    :param content: list of strings, selection of content types to load

    This generator yields :py:class:`pyrocko.squirrel.Nut` objects for
    individual pieces of information found when reading the given files. Such a
    nut may represent a waveform, a station, a channel, an event or other data
    type. The nut itself only contains the meta-information. The actual content
    information is attached to the nut if requested. All nut meta-information
    is stored in the squirrel meta-information database. If possible, this function
    avoids accessing the actual disk files and provides the requested
    information straight from the database. Modified files are recognized and
    reindexed as needed. 
    '''

    from ..base import Selection

    n_db = 0
    n_load = 0
    selection = None

    if isinstance(paths, (str, newstr)):
        paths = [paths]
    else:
        if segment is not None:
            raise TypeError(
                'iload: segment argument can only be used when loading from '
                'a single file')

        if isinstance(paths, Selection):
            selection = paths
            if database is not None:
                raise TypeError(
                    'iload: database argument must be None when called with a '
                    'selection')

            database = selection.get_database()

    temp_selection = None
    if database:
        if not selection:
            temp_selection = database.new_selection(paths, state=1)
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

        it = ((path, []) for path in paths)

    n_files = 0
    for path, old_nuts in it:
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
                                 % path)

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
                    format_this = detect_format(path)
            else:
                format_this = format

            mod = get_backend(format_this)
            mtime, size = mod.get_stats(path)

            logger.debug('reading file %s' % path)
            nuts = []
            for nut in mod.iload(format_this, path, segment, content):
                nut.file_path = path
                nut.file_format = format_this
                nut.file_mtime = mtime
                nut.file_size = size

                nuts.append(nut)
                n_load += 1
                yield nut

            if database and nuts != old_nuts:
                if segment is not None:
                    nuts = mod.iload(format_this, path, None, [])
                    for nut in nuts:
                        nut.file_path = path
                        nut.file_format = format_this
                        nut.file_mtime = mtime

                database.dig(nuts)

        except FileLoadError:
            logger.error('an error occured while reading file: %s' % path)
            if database:
                database.reset(path)

    if database:
        if commit:
            database.commit()

        if temp_selection:
            del temp_selection

    logger.debug('iload: from db: %i, from files: %i, files: %i' % (
        n_db, n_load, n_files))


__all__ = [
    'iload',
    'detect_format',
    'get_backend',
    'FormatDetectionFailed',
    'UnknownFormat',
]
