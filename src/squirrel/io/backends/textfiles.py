from pyrocko.io.io_common import get_stats  # noqa
from pyrocko.squirrel import model
import logging
from builtins import str as newstr

logger = logging.getLogger('pyrocko.squirrel.io.textfiles')


def provided_formats():
    return ['pyrocko_stations']


def detect_pyrocko_stations(first512):
    first512 = first512.decode('utf-8')
    for line in first512.splitlines():
        t = line.split(None, 5)
        if len(t) in (5, 6):
            if len(t[0].split('.')) != 3:
                return False

            try:
                lat, lon, ele, dep = map(float, t[1:5])
                if lat < -90. or 90 < lat:
                    return False
                if lon < -180. or 180 < lon:
                    return False

                return True

            except Exception:
                raise
                return False

    return False


def detect(first512):
    if detect_pyrocko_stations(first512):
        return 'pyrocko_stations'

    return None


def float_or_none(s):
    if s.lower() == 'nan':
        return None
    else:
        return float(s)


def iload(format, file_path, segment, content):
    inut = 0
    tmin = None
    tmax = None
    with open(file_path, 'r') as f:

        have_station = False
        for (iline, line) in enumerate(f):
            try:
                toks = line.split(None, 5)
                if len(toks) == 5 or len(toks) == 6:
                    net, sta, loc = toks[0].split('.')
                    lat, lon, elevation, depth = [float(x) for x in toks[1:5]]
                    if len(toks) == 5:
                        description = u''
                    else:
                        description = newstr(toks[5])

                    agn = ('', 'FDSN')[net != '']

                    nut = model.make_station_nut(
                        file_segment=0,
                        file_element=inut,
                        agency=agn,
                        network=net,
                        station=sta,
                        location=loc,
                        tmin=tmin,
                        tmax=tmax)

                    if 'station' in content:
                        nut.content = model.Station(
                            lat=lat,
                            lon=lon,
                            elevation=elevation,
                            depth=depth,
                            description=description,
                            **nut.station_kwargs)

                    yield nut
                    inut += 1

                    have_station = True

                elif len(toks) == 4 and have_station:
                    cha = toks[0]
                    azi = float_or_none(toks[1])
                    dip = float_or_none(toks[2])
                    gain = float(toks[3])

                    if gain != 1.0:
                        logger.warning(
                            '%s.%s.%s.%s gain value from stations '
                            'file ignored - please check' % (
                                        net, sta, loc, cha))

                    nut = model.make_channel_nut(
                        file_segment=0,
                        file_element=inut,
                        agency=agn,
                        network=net,
                        station=sta,
                        location=loc,
                        channel=cha,
                        tmin=tmin,
                        tmax=tmax)

                    if 'channel' in content:
                        nut.content = model.Channel(
                            lat=lat,
                            lon=lon,
                            elevation=elevation,
                            depth=depth,
                            azimuth=azi,
                            dip=dip,
                            **nut.channel_kwargs)

                    yield nut
                    inut += 1

                else:
                    raise Exception('invalid syntax')

            except Exception as e:
                logger.warning(
                    'skipping invalid station/channel definition: %s '
                    '(line: %i, file: %s' % (str(e), iline, file_path))
