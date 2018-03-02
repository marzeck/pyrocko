'''Prompt seismological data access with a fluffy tail.

Usage
-----

.. code::

    from pyrocko import squirrel as psq

    sq = psq.Squirrel()
    sq.add(files)


Concepts
--------

* squirrel
* nut
* database

Reference
---------
'''

from . import base, model, io

from .base import *  # noqa
from .model import *  # noqa
from .io import *  # noqa

__all__ = base.__all__ + model.__all__ + io.__all__
