import os
import logging

from pyrocko.io_common import FileLoadError
from pyrocko.squirrel.io import mseed, sac, datacube, stationxml, textfiles
from builtins import str as newstr

backend_modules = [mseed, sac, datacube, stationxml, textfiles]


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


def detect_format(filename):
    '''Determine file type from first 512 bytes.'''

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


class FormatDetectionFailed(FileLoadError):
    def __init__(self, filename):
        FileLoadError.__init__(
            self, 'format detection failed for file: %s' % filename)


class UnknownFormat(FileLoadError):
    def __init__(self, format):
        FileLoadError.__init__(
            self, 'unknown format: %s' % format)


def get_mtime(filename):
    try:
        return os.stat(filename)[8]
    except OSError as e:
        raise FileLoadError(e)


def iload(
        filenames,
        segment=None,
        format='detect',
        squirrel=None,
        check_mtime=True,
        commit=True,
        skip_up_to_date=False,
        content=['waveform', 'station', 'channel', 'response', 'event']):

    '''
    Iteratively load content or index from files.

    :param filenames: iterator yielding strings, filenames to load from
    :param segment: ``str`` file-specific segment identifier (con only be used
        when loading from a single file.
    :param format: ``str`` file format or ``'detect'`` for autodetection
    :param squirrel: :py:class:`pyrocko.squirrel.Squirrel` object to use
        for index cache
    :param check_mtime: ``bool`` flag, whether to check the modification time
        of every file
    :param commit: ``bool`` flag, whether to commit updated information to the
        index cache
    :param skip_up_to_date: ``bool`` flag, if ``True``, only yield index nuts
        for new / modified files
    :param content: list of strings, selection of content types to load
    '''

    n_db = 0
    n_load = 0

    if isinstance(filenames, (str, newstr)):
        filenames = [filenames]
    else:
        if segment is not None:
            raise TypeError(
                'iload: segment argument can only be used when loading from '
                'a single file')

    selection = None
    if squirrel:
        selection = squirrel.new_selection(filenames)
        if skip_up_to_date:
            selection_filt = selection.filter_modified_or_new(check_mtime)
            selection.delete()
            selection = selection_filt

        it = selection.undig()

    else:
        if skip_up_to_date:
            raise TypeError(
                'iload: skip_up_to_date argument requires squirrel')

        it = ((fn, []) for fn in filenames)

    for filename, old_nuts in it:
        mtime = None
        if check_mtime and old_nuts:
            mtime = get_mtime(filename)
            if mtime != old_nuts[0].file_mtime:
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
                        squirrel.undig_content(nut)

                    n_db += 1
                    yield nut

                continue

        if mtime is None:
            mtime = get_mtime(filename)

        if format == 'detect':
            if old_nuts and old_nuts[0].file_mtime == mtime:
                format_this = old_nuts[0].file_format
            else:
                format_this = detect_format(filename)
        else:
            format_this = format

        if format_this not in format_providers:
            raise UnknownFormat(format_this)

        mod = format_providers[format_this][0]

        logger.debug('reading file %s' % filename)
        nuts = []
        for nut in mod.iload(format_this, filename, segment, content):
            nut.file_name = filename
            nut.file_format = format_this
            nut.file_mtime = mtime

            nuts.append(nut)
            n_load += 1
            yield nut

        if squirrel and nuts != old_nuts:
            if segment is not None:
                nuts = mod.iload(format_this, filename, None, [])
                for nut in nuts:
                    nut.file_name = filename
                    nut.file_format = format_this
                    nut.file_mtime = mtime

            squirrel.dig(nuts)

    if squirrel:
        if commit:
            squirrel.commit()

        if selection:
            selection.delete()

    logger.debug('iload: from db: %i, from files: %i' % (n_db, n_load))
