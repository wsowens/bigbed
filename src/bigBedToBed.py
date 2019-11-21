#!/usr/bin/env python3
from collections import namedtuple
BIGBEDSIG = 0x8789F2EB

ZoomLevel = namedtuple("ZoomLevel", ["reduction_level", "reserved",
                                     "dataOffset", "indexOffset"])


class BigBed:
    '''a BigBed file'''
    def __init__(self):
        self.type_sig = BIGBEDSIG
        self.is_swapped = False
        # read in all the header information
        # may need to include byte swapped data