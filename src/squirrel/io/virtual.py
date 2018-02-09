from collections import defaultdict
from pyrocko.io.io_common import FileLoadError


def provided_formats():
    return ['virtual']


def detect(first512):
    return None


class UniqueKeyRequired(Exception):
    pass


def get_mtime(file_name):
    try:
        return data_mtimes[file_name]
    except KeyError:
        raise FileLoadError(file_name)


data = defaultdict(list)
data_mtimes = {}


def add_nuts(nuts):
    fns = set()
    for nut in nuts:
        fns.add(nut.file_name)
        data[nut.file_name].append(nut)

    for fn in fns:
        data[fn].sort(key=lambda nut: (nut.file_segment, nut.file_element))
        ks = set()
        for nut in data[fn]:
            k = nut.file_segment, nut.file_element
            if k in ks:
                raise UniqueKeyRequired()

            ks.add(k)

        old_mtime = data_mtimes.get(fn, 0)
        data_mtimes[fn] = old_mtime + 1


def iload(format, filename, segment, content):
    assert format == 'virtual'

    for nut in data[filename]:
        yield nut
