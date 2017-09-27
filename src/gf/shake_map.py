import numpy as num
from pyrocko.guts import Object, StringChoice
from pyrocko.guts_array import Array
from pyrocko import gf

guts_prefix = 'pf'


class ShakeMapQuantity(StringChoice):
    choices = ['pga', 'pgv', 'intensity']


class ShakeMap(Object):
    source = gf.Source.T(optional=True)
    origin = gf.Location.T()
    norths = Array.T(
        dtype=num.float, shape=(None,), serialize_as='base64+meta')
    easts = Array.T(
        dtype=num.float, shape=(None,), serialize_as='base64+meta')
    values = Array.T(
        dtype=num.float, shape=(None, None), serialize_as='base64+meta')
    quantity = ShakeMapQuantity.T(default='pga')
