import logging

from pyrocko.io_common import FileLoadError
from pyrocko.squirrel.squirrel import Selection
from pyrocko.squirrel.io import mseed, sac, datacube, stationxml, textfiles, \
    virtual
from builtins import str as newstr

backend_modules = [mseed, sac, datacube, stationxml, textfiles, virtual]


logger = logging.getLogger('pyrocko.sqirrel.io')


def update_format_providers():
    '''Update global mapping from file format to io backend module.'''

    global format_providers
    format_providers = {}
    for mod in backend_modules:
        for format in mod.provided_formats():
            if format not in format_providers:
                format_providers[format] = []

            format_providers[format].append(mod)


format_providers = {}
update_format_providers()


class FormatDetectionFailed(FileLoadError):
    def __init__(self, filename):
        FileLoadError.__init__(
            self, 'format detection failed for file: %s' % filename)


class UnknownFormat(FileLoadError):
    def __init__(self, format):
        FileLoadError.__init__(
            self, 'unknown format: %s' % format)


def get_format_provider(fmt):
    try:
        return format_providers[fmt][0]
    except KeyError:
        raise UnknownFormat(fmt)


def detect_format(filename):
    '''Determine file type from first 512 bytes.'''

    if filename.startswith('virtual:'):
        return 'virtual'

    try:
        with open(filename, 'rb') as f:
            data = f.read(512)

    except OSError as e:
        raise FileLoadError(e)

    fmt = None
    for mod in backend_modules:
        fmt = mod.detect(data)
        if fmt is not None:
            return fmt

    raise FormatDetectionFailed(filename)


def iload(
        filenames,
        segment=None,
        format='detect',
        database=None,
        check_mtime=True,
        commit=True,
        skip_unchanged=False,
        content=['waveform', 'station', 'channel', 'response', 'event']):

    '''
    Iteratively load content or index from files.

    :param filenames: iterator yielding filenames to load from or
        :py:class:`pyrocko.squirrel.Selection` object
    :param segment: ``str`` file-specific segment identifier (con only be used
        when loading from a single file.
    :param format: ``str`` file format or ``'detect'`` for autodetection
    :param database: :py:class:`pyrocko.squirrel.Database` object to use
        as index cache
    :param check_mtime: ``bool`` flag, whether to check the modification time
        of every file
    :param commit: ``bool`` flag, whether to commit updated information to the
        index cache
    :param skip_unchanged: ``bool`` flag, if ``True``, only yield index nuts
        for new / modified files
    :param content: list of strings, selection of content types to load
    '''

    n_db = 0
    n_load = 0
    selection = None

    if isinstance(filenames, (str, newstr)):
        filenames = [filenames]
    else:
        if segment is not None:
            raise TypeError(
                'iload: segment argument can only be used when loading from '
                'a single file')

        if isinstance(filenames, Selection):
            selection = filenames
            if database is not None:
                raise TypeError(
                    'iload: database argument must be None when called with a '
                    'selection')

            database = selection.database()

    temp_selection = None
    if database:
        if not selection:
            temp_selection = database.new_selection(filenames)
            selection = temp_selection

        if skip_unchanged:
            selection.flag_unchanged(check_mtime)
            it = selection.undig_grouped(skip_unchanged=True)
        else:
            it = selection.undig_grouped()

    else:
        if skip_unchanged:
            raise TypeError(
                'iload: skip_unchanged argument requires database')

        it = ((fn, []) for fn in filenames)

    for filename, old_nuts in it:
        if check_mtime and old_nuts and old_nuts[0].file_modified():
            old_nuts = []

        if segment is not None:
            old_nuts = [nut for nut in old_nuts if nut.segment == segment]

        if old_nuts:
            db_only_operation = not content or all(
                nut.kind in content and nut.content_in_db for nut in old_nuts)

            if db_only_operation:
                logger.debug('using cached information for file %s, '
                             % filename)

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
                format_this = detect_format(filename)
        else:
            format_this = format

        mod = get_format_provider(format_this)
        mtime = mod.get_mtime(filename)

        logger.debug('reading file %s' % filename)
        nuts = []
        for nut in mod.iload(format_this, filename, segment, content):
            nut.file_name = filename
            nut.file_format = format_this
            nut.file_mtime = mtime

            nuts.append(nut)
            n_load += 1
            yield nut

        if database and nuts != old_nuts:
            if segment is not None:
                nuts = mod.iload(format_this, filename, None, [])
                for nut in nuts:
                    nut.file_name = filename
                    nut.file_format = format_this
                    nut.file_mtime = mtime

            database.dig(nuts)

    if database:
        if commit:
            database.commit()

        if temp_selection:
            temp_selection.delete()

    logger.debug('iload: from db: %i, from files: %i' % (n_db, n_load))
