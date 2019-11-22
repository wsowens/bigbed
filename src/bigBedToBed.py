#!/usr/bin/env python3
import sys
import struct
from collections import namedtuple
BIGBEDSIG = b'\x87\x89\xF2\xEB'
BPTSIG = b'\x78\xCA\x8C\x91'

ZoomLevel = namedtuple("ZoomLevel", ["reduction_level", "reserved",
                                     "data_offset", "index_offset"])
ChromInfo = namedtuple("ChromInfo", ["name", "id", "size"])

def read_file(handle, bytes, is_swapped = False):
    '''read [bytes] from [handle], swap if is_swapped is true'''

class Handle:
    def __init__(self):
        raise TypeError("This is intended to be abstract")

    def read_bytes(self, n):
        """read n bytes from the file, swapping as necessary"""
        data = self._handle.read(n)
        if self.verbose:
            print(data)
        return data

    def read_char(self):
        data = self.read_bytes(1)
        if self.is_swapped:
            data = data[::-1]
        return struct.unpack(">c", data)[0]

    def read_short(self):
        data = self.read_bytes(2)
        if self.is_swapped:
            data = data[::-1]
        return struct.unpack(">H", data)[0]
    
    def read_long(self):
        data = self.read_bytes(4)
        if self.is_swapped:
            data = data[::-1]
        return struct.unpack(">L", data)[0]
    
    def read_longlong(self):
        data = self.read_bytes(8)
        if self.is_swapped:
            data = data[::-1]
        return struct.unpack(">Q", data)[0]
            

class BPlusTree(Handle):
    def __init__(self, filename, handle):
        self.verbose = False
        self.filename = filename
        self._handle = handle # hmmm how to approach this in Rust
        sig = self._handle.read(4)
        if sig == BPTSIG:
            self.is_swapped = False
        elif sig[::-1] == BPTSIG:
            self.is_swapped = True
        else:
            raise ValueError(f"This is not a B-plus tree file! Expected: {BIGBEDSIG}; Received: {sig}")
        
        # Read rest of defined bits of header, byte swapping as needed.
        self.blockSize = self.read_long()
        self.keySize = self.read_long()
        self.valSize = self.read_long()
        self.itemCount = self.read_longlong()

        # skip over the reserved region
        self._handle.seek(4, 1)
        self._handle.seek(4, 1)

        self.rootOffset = self._handle.tell()
    
    def chrom_list(self):
        chroms = []
        def get_chrom(key, val, keySize, valSize):
            name = key
            if self.is_swapped:
                val = struct.unpack("<LL", val)
            else:
                val = struct.unpack(">LL", val)
            chrom_id = val[0]
            chrom_size = val[1]
            chroms.append(ChromInfo(name, chrom_id, chrom_size))
        self.traverse_tree(get_chrom)
        return chroms

    def traverse_tree(self, callback):
        self._rTraverse(self.rootOffset, callback)
    
    def _rTraverse(self, start, callback):
        self._handle.seek(start)

        is_leaf = self.read_char()
        reserved = self.read_char()
        child_count =  self.read_short()

        if self.verbose:
            print(f"is_leaf: {is_leaf}")
            print(f"reserved: {reserved}")
            print(f"child_count: {child_count}")

        if (is_leaf):
            for _ in range(child_count):
                if self.verbose:
                    print("found leaf")
                key = self.read_bytes(self.keySize)
                val = self.read_bytes(self.valSize)
                callback(key, val, self.keySize, self.valSize)
        else:
            fileOffsets = []
            for _ in range(child_count):
                key = self.read_bytes(self.keySize)
                fileOffsets.append(struct.unpack(">Q", self.read_bytes(8)))
            # now loop through each offset
            print("found internal")
            print(fileOffsets)
            for offset in fileOffsets:
                print(f"recursing [{offset}]")
                self._rTraverse(offset, callback)


class BigBed(Handle):
    '''a BigBed file'''
    def __init__(self, filename):
        # copy over the filename
        self.verbose = False
        self.filename = filename
        self._handle = open(filename, 'rb')

        self.type_sig = BIGBEDSIG
        sig = self._handle.read(4)
        if sig == BIGBEDSIG:
            self.is_swapped = False
        elif sig[::-1] == BIGBEDSIG:
            self.is_swapped = True
        else:
            raise ValueError(f"This is not a BigBed file! Expected: {BIGBEDSIG}; Received: {sig}")
        print(sig)

        # read in all the header information
        # may need to include byte swapped data
        self.version = self.read_short()
        self.zoomLevels = self.read_short()
        self.chromTreeOffset = self.read_longlong()
        self.unzoomedDataOffset = self.read_longlong()
        self.unzoomedIndexOffset = self.read_longlong()
        self.fieldCount = self.read_short()
        self.definedFieldCount = self.read_short()
        self.asOffset = self.read_longlong()
        self.totalSummaryOffset = self.read_longlong()
        self.uncompressBufSize = self.read_long()
        self.extensionOffset = self.read_longlong()

        self.levelList = []
        print(self.extensionOffset)
        # now handle the zoom headers
        for _ in range(self.zoomLevels):
            new_level = ZoomLevel(
                reduction_level=self.read_long(),
                reserved=self.read_long(),
                data_offset=self.read_longlong(),
                index_offset=self.read_longlong(),
            )
            self.levelList.append(new_level)
        
        # now handle the extension shit
        if (self.extensionOffset != 0):
            self._handle.seek(self.extensionOffset)
            self.extensionSize = self.read_short()
            self.extraIndexCount = self.read_short()
            self.extraIndexListOffset = self.read_longlong()
        
        # see to the B+ tree file region
        self._handle.seek(self.chromTreeOffset)
        # attach the B+ tree
        self.chromBpt = BPlusTree(filename, self._handle)
    
    def query(self, chrom, start, end, max_items):
        pass

    def to_bed(self, maxItems = 0):
        chroms = self.chromBpt.chrom_list()

        itemCount = 0
        for chrom_name, chrom_start, chrom_size in chroms:
            # fix this to take parameters later?
            name = chrom_name
            start = chrom_start
            end = chrom_size

            # 0 actually means unlimited items
            itemsLeft = 0
            if maxItems != 0:
                itemsLeft = maxItems - itemCount
            
            if (itemsLeft < 0):
                break

            intervals = self.query(name, start, end, itemsLeft)
        
    def __del__(self):
        self._handle.close()


def dump_traits(obj):
    for k in dir(obj):
        if not k.startswith("_"):
            print(f"{k}: {getattr(obj, k)}")

if __name__ == "__main__":
    bb = BigBed(sys.argv[1])
    #dump_traits(bb.chromBpt)
    bb.chromBpt.verbose = True
    chroms = bb.chromBpt.chrom_list()