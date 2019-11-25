#!/usr/bin/env python3
import sys
import struct
from collections import namedtuple
from enum import Enum
import zlib

BIGBEDSIG = b'\x87\x89\xF2\xEB'
BPTSIG = b'\x78\xCA\x8C\x91'
CIRTREESIG = b'\x24\x68\xAC\xE0'

ZoomLevel = namedtuple("ZoomLevel", ["reduction_level", "reserved",
                                     "data_offset", "index_offset"])


ChromInfo = namedtuple("ChromInfo", ["name", "id", "size"])


FileOffsetSize = namedtuple("FileOffsetSize", ["offset", "size"])

BedLine = namedtuple("BedLine", ["start", "end", "rest", "chrom_id"])


class VL(Enum):
    """verbose levels"""
    READ = 0
    TREE = 1
    GENERIC = 2

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def strlen(s):
    """scuffed implementation of strlen, measures number of bytes until first \0"""
    index = 0
    for index, char in enumerate(s):
        if char == 0:
            index -= 1
            break
    return index

def cmpTwo(aHi, aLo, bHi, bLo):
    if (aHi < bHi):
        return 1
    elif (aHi > bHi):
        return -1
    else:
        if (aLo < bLo):
            return 1
        elif (aLo > bLo):
            return -1
        else:
            return 0

def cirTreeOverlaps(qChrom, qStart, qEnd,
                    rStartChrom, rStartBase, rEndChrom, rEndBase):
    """really scuffed function"""
    #print(cmpTwo(qChrom, qStart, rEndChrom, rEndBase) > 0)
    #print(cmpTwo(qChrom, qEnd, rStartChrom, rStartBase) < 0)
    return cmpTwo(qChrom, qStart, rEndChrom, rEndBase) > 0 and cmpTwo(qChrom, qEnd, rStartChrom, rStartBase) < 0

class Handle:
    def __init__(self):
        raise TypeError("This is intended to be abstract")

    def read_bytes(self, n):
        """read n bytes from the file, swapping as necessary"""
        data = self._handle.read(n)
        self.vprint(data, vl=VL.READ)
        return data

    def read_char(self):
        data = self.read_bytes(1)
        if self.is_swapped:
            data = data[::-1]
        return struct.unpack(">c", data)[0]
    
    def read_bool(self):
        data = self.read_char()
        return data != b'\x00'

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
    
    def vprint(self, *args, vl=VL.GENERIC, **kwargs):
        """print to stderr if verbose is enabled"""
        if vl in self.vls:
            print(*args, file=sys.stderr, **kwargs)
            

class BPlusTree(Handle):
    def __init__(self, filename, handle, verbose = None):
        self.vls = verbose
        self.filename = filename
        self._handle = handle # hmmm how to approach this in Rust
        sig = self._handle.read(4)
        if sig == BPTSIG:
            self.is_swapped = False
        elif sig[::-1] == BPTSIG:
            self.is_swapped = True
        else:
            raise ValueError(f"This is not a B-plus tree file! Expected: {BPTSIG}; Received: {sig}")
        
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

    def find(self, key):
        """find value for 'key'"""
        return self._find_maybe_multi(key, False)
    
    def _find_maybe_multi(self, key, multi: bool):
        """key should be some kind of bytes like object"""
        # compare the size of the key to max keysize:
        if len(key) > self.keySize:
            self.vprint(f"key too long {key} > {self.keySize}")
            return None
        # zero extend the key if necessary
        if len(key) != self.keySize:
            key = key + (b"\x00" * (self.keySize - len(key)))
            self.vprint(f"extended key: {repr(key)}")

        # COMPARE THE EXPECTED SIZE OF THE VALUE TO THE VALUE SIZE IN THE TREE
        # NOTE, WE CANNNOT DO THIS EASILY IN PYTHON
        # SEE bPlusTree.c FOR MORE INFORMATION

        if (multi):
            # self._r_find_multi
            return NotImplemented
        else:
            # return result... may be None
            return self._r_find(self.rootOffset, key)
        
    def _r_find(self, offset, key):
        # move to the start of the block
        self._handle.seek(offset)

        # read the block header
        is_leaf = self.read_char()
        reserved = self.read_char()
        child_count = self.read_short()

        self.vprint(f"rFindBpt, {offset} {key} childCount {child_count} isLeaf {is_leaf}", vl=VL.TREE)
        # some bullshit about putting the key on the stack

        if is_leaf:
            # loop through the 
            for _ in range(child_count):
                other_key = self.read_bytes(self.keySize)
                val = self.read_bytes(self.valSize)
                if key == other_key:
                    return val
            # if code reaches this point, then key was not found
            return None
        else:
            # read and discard the first key
            self.read_bytes(self.keySize)

            # scan for first file offset
            fileOffset = self.read_longlong()

            # loop through the remainder, break when a key bigger than this one is found
            for _ in range(child_count):
                other_key = self.read_bytes(self.keySize)
                if key < other_key:
                    break
                fileOffset = self.read_longlong()

            return self._r_find(fileOffset, key)

    def traverse_tree(self, callback):
        self._rTraverse(self.rootOffset, callback)
    
    def _rTraverse(self, start, callback):
        self._handle.seek(start)

        is_leaf = self.read_char()
        reserved = self.read_char()
        child_count =  self.read_short()

        self.vprint(f"is_leaf: {is_leaf}")
        self.vprint(f"reserved: {reserved}")
        self.vprint(f"child_count: {child_count}")

        if (is_leaf):
            for _ in range(child_count):
                if self.vls:
                    self.vprint("found leaf")
                key = self.read_bytes(self.keySize)
                val = self.read_bytes(self.valSize)
                callback(key, val, self.keySize, self.valSize)
        else:
            fileOffsets = []
            for _ in range(child_count):
                key = self.read_bytes(self.keySize)
                fileOffsets.append(struct.unpack(">Q", self.read_bytes(8)))
            # now loop through each offset
            self.vprint("found internal")
            self.vprint(fileOffsets)
            for offset in fileOffsets:
                self.vprint(f"recursing [{offset}]")
                self._rTraverse(offset, callback)

class CIRTree(Handle):
    def __init__(self, filename, handle, verbose = None):
        if verbose is None:
            verbose = set()
        self.vls = verbose
        self.filename = filename
        self._handle = handle # hmmm how to approach this in Rust
        sig = self._handle.read(4)
        if sig == CIRTREESIG:
            self.is_swapped = False
        elif sig[::-1] == CIRTREESIG:
            self.is_swapped = True
        else:
            raise ValueError(f"This is not a B-plus tree file! Expected: {CIRTREESIG}; Received: {sig}")

        self.blockSize = self.read_long()
        self.itemCount = self.read_longlong()
        self.startChromIx = self.read_long()
        self.startBase = self.read_long()
        self.endChromIx = self.read_long()
        self.endBase = self.read_long()
        self.fileSize = self.read_longlong()
        self.itemsPerSlot = self.read_long()

        # skip over the reserved bits of the header
        self._handle.seek(4, 1)
        self.rootOffset = self._handle.tell()

    def find_blocks(self, chrom_id, start, end):
        """find overlapping blocks"""
        results = []
        self._find(0, self.rootOffset, chrom_id, start, end, results)
        return results

    def _find(self, level: int, offset: int, chrom_id: int, start: int, end: int, results: list):
        """find overlapping blocks"""
        self._handle.seek(offset)

        # read the block header
        is_leaf = self.read_bool()
        reserved = self.read_char()
        child_count = self.read_short()

        self.vprint(f"rFindOverlappingBlocks, {offset} {chrom_id}:{start}-{end}. childCount {child_count} isLeaf {is_leaf}", vl=VL.TREE)
        
        if is_leaf:
            for _ in range(child_count):
                startChromIx = self.read_long()
                startBase = self.read_long()
                endChromIx = self.read_long()
                endBase = self.read_long()
                offset = self.read_longlong()
                size = self.read_longlong()
                self.vprint(f"{_}: {startChromIx}, {startBase}--{endChromIx},{endBase}")
                # if the block overlaps, add it to the list of results
                if cirTreeOverlaps(chrom_id, start, end, startChromIx, startBase, endChromIx, endBase):
                    self.vprint("overlaps")
                    block = FileOffsetSize(offset, size)
                    results.append(block)
        else:
            # diverging from cirTree.c:
            # instead of creating 5 different arrays, we will just create one and use a tuple
            
            # read node into lists
            node_data = []
            for _ in range(child_count):
                node_data.append((
                    self.read_long(),     # startChromIx
                    self.read_long(),     # startBase
                    self.read_long(),     # endChromIx
                    self.read_long(),     # endbase
                    self.read_longlong(), # offset
                ))

            for startChromIx, startBase, endChromIx, endBase, offset in node_data:
                # if the tree overlaps, recurse
                if cirTreeOverlaps(chrom_id, start, end, startChromIx, startBase, endChromIx, endBase):
                    self._find(level+1, offset, chrom_id, start, end, results)


def fileOffsetSizeFindGap(blocks):
    """returns the indices that the block starts and stops with"""
    beforeGap = 0
    afterGap = 0
    for index, block in enumerate(blocks):
        nxt = index + 1
        if (nxt >= len(blocks) or blocks[nxt].offset != block.offset + block.size):
            beforeGap = index
            afterGap = nxt
            return beforeGap, afterGap
    return index, nxt

class BigBed(Handle):
    '''a BigBed file'''
    def __init__(self, filename, verbose = None):
        # copy over the filename
        if verbose is None:
            verbose = set()
        self.vls = verbose
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
        self.chromBpt = BPlusTree(filename, self._handle, verbose=verbose)

        # creating unzoomedCir field
        self.unzoomedCir = None

    def overlapping_blocks(self, ctf: CIRTree, chrom: str, start: int, end: int):
        """find overlapping blocks in this BigBed with the provided parameters
        returns a list of blocks and chromSize info"""
        # first: find the chromosome information in the B+ tree
        idSize = self.chromBpt.find(chrom)       
        if idSize is None:
            if chrom.startswith("chr"):
                idSize = self.chromBpt.find(chrom[3:])
                if idSize is None:
                    return None
            else:
                return None
        # read the value of the result
        if self.chromBpt.is_swapped:
            val = struct.unpack("<LL", idSize)
        else:
            val = struct.unpack(">LL", idSize)
        chrom_id = val[0]
        chrom_size = val[1]
       
        # return the chromID and the results from the R tree
        self.vprint(f"find_blocks({chrom_id}, {start}, {end})")
        blocks = ctf.find_blocks(chrom_id, start, end)
        if blocks is None:
            return None
        return blocks, chrom_id
       
    def query(self, chrom, start, end, max_items):
        intervals = []
        # attach unzoomed circle if it is not already present
        if self.unzoomedCir is None:
            self._handle.seek(self.unzoomedIndexOffset)
            self.unzoomedCir = CIRTree(self.filename, self._handle, verbose = self.vls)
        # from kent:
        # "Find blocks with padded start and end to make sure we include zero-length insertions"
        paddedStart = start - 1 if start > 0 else start
        paddedEnd = end + 1

        result = self.overlapping_blocks(self.unzoomedCir, chrom, paddedStart, paddedEnd)
        
        if result is None:
            # do something!
            exit(-1)
        blocks, chrom_id = result

        # slice index for reading things out of memory
        toSwap = -1 if self.is_swapped else 1
        
        # "set up for uncompression optionally"
        uncompressBuf = None
        if self.uncompressBufSize > 0:
            self.vprint("allocating memory for uncompression")
            uncompressBuf = b"\x00" * self.uncompressBufSize

        self.vprint(blocks)
        itemCount = 0
        # while there are blocks in the list
        while blocks:
            # find contigious blocks and read them into mergedBuf
            # this is done to prevent consecutive reads
            beforeGap, afterGap = fileOffsetSizeFindGap(blocks)
            # get the first offset
            mergedOffset = blocks[0].offset
            mergedSize = blocks[beforeGap].offset + blocks[beforeGap].size - mergedOffset
            self._handle.seek(mergedOffset)
            # read the blocks
            mergedBuf = self.read_bytes(mergedSize)
            blockBuf = mergedBuf



            # iterate over the indices until you reach the gap
            blockPt = 0
            for index in range(afterGap):
                # if we have any uncompression to do:
                if uncompressBuf:
                    # we do it this way so that we can keep the blocks intact!
                    self.vprint("reading compressed data")
                    buff = zlib.decompress(mergedBuf[blockPt:blocks[index].size])
                    blockEnd = blockPt + len(buff)
                ## otherwise prepare the buffer as normal
                else:
                    buff = mergedBuf
                    blockEnd = blockPt + blocks[index].size
                while (blockPt < blockEnd):
                    self.vprint(f"blockPt: {blockPt}; toSwap: {toSwap}")
                    self.vprint(buff[blockPt:blockPt+4])
                    chr = struct.unpack(">L", buff[blockPt:blockPt+4][::toSwap])[0]
                    blockPt += 4
                    s = struct.unpack(">L", buff[blockPt:blockPt+4][::toSwap])[0]
                    blockPt += 4
                    e = struct.unpack(">L", buff[blockPt:blockPt+4][::toSwap])[0]
                    blockPt += 4

                    # find the next null character
                    restLen = strlen(buff[blockPt:])

                    # check if this is in our range (note that the or is for zero elements)
                    if (chr == chrom_id and ((s < end and e > start) or (s == e and (s == end or end == start)))):
                        itemCount += 1
                        if (max_items > 0 and itemCount > max_items):
                            break
                        # check for any extra fields
                        rest = None
                        if (restLen > 0):
                            # up to but not including the null character
                            rest = buff[blockPt:restLen+1]
                        # add data to struct
                        data = BedLine(start=s, end=e, chrom_id=chr, rest = None)
                        self.vprint(data)
                        intervals.append(data)

                    # restLen + 1 will be at the null character
                    # restLen + 2 will be just past it
                    blockPt += restLen + 2
                    
                if (max_items > 0 and itemCount > max_items):
                    break
                # prepare the block for the next round
            if (max_items > 0 and itemCount > max_items):
                break
            # pop off all the undesired blocks
            blocks = blocks[afterGap:]
        # deallocate everything
        return intervals

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

            chrom_name = chrom_name.strip(b'\x00').decode('ascii')
            intervals = self.query(name, start, end, itemsLeft)
            for start, end, rest, chrom_id in intervals:
                if rest is None:
                    print(f"{chrom_name}\t{start}\t{end}")
                else:
                    print(f"{chrom_name}\t{start}\t{end}\t{rest}")

        
    def __del__(self):
        self._handle.close()


def dump_traits(obj):
    for k in dir(obj):
        if not k.startswith("_"):
            print(f"{k}: {getattr(obj, k)}")

if __name__ == "__main__":
    bb = BigBed(sys.argv[1])
    bb.to_bed()
    #dump_traits(bb.chromBpt)
    #chroms = bb.chromBpt.chrom_list()
    #bb.chromBpt.find(b"chr1")
    # attaching the index manually
    #bb._handle.seek(bb.unzoomedIndexOffset)
    #bb.unzoomedCir = CIRTree(bb.filename, bb._handle, {VL.GENERIC, VL.TREE})
    #x = bb.query(b"chr1", 10, 31796156, 0)