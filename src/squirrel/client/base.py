
class Selection(object):

    def __init__(self, tmin=None, tmax=None):
        self.tmin = tmin
        self.tmax = tmax

    def contains(self, selection):
        # tmin, tmax = None, None is considered -inf, inf

        if self.tmin is not None and selection.tmin is not None:
            b1 = self.tmin <= selection.tmin
        elif self.tmin is None:
            b1 = True
        else:
            b1 = False

        if self.tmax is not None and selection.tmax is not None:
            b2 = selection.tmax <= self.tmax
        elif self.tmax is None:
            b2 = True
        else:
            b2 = False

        return b1 and b2

    def add(self, selection):
        if selection.tmin is None or self.tmin is None:
            self.tmin = None
        else:
            self.tmin = min(selection.tmin, self.tmin)

        if selection.tmax is None or self.tmax is None:
            self.tmax = None
        else:
            self.tmax = min(selection.tmax, self.tmax)


class Source(object):


    def update_channel_inventory(self, squirrel, selection):

        '''
        Let local inventory be up-to-date with remote for a given selection.
        '''

        pass
