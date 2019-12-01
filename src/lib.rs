extern crate flate2;

pub mod error;
use crate::error::Error::{self, *};

use std::io::{Read, Seek, SeekFrom, Write};
use std::collections::VecDeque;
use std::convert::TryInto;
use flate2::{Decompress, FlushDecompress};


static BIGBED_SIG: [u8; 4] = [0x87, 0x89, 0xF2, 0xEB];
static BPT_SIG: [u8; 4] = [0x78, 0xCA, 0x8C, 0x91];
static CIRTREE_SIG: [u8; 4] = [0x24, 0x68, 0xAC, 0xE0];


/// a collection of useful methods for producing bytes from a type that implements Read
trait ByteReader: Read {
    fn read_u64(&mut self, big_endian: bool) -> u64 {
        let mut bytes: [u8; 8] = [0;8];
        self.read_exact(&mut bytes).unwrap();

        if big_endian {
            u64::from_be_bytes(bytes)
        } else {
            u64::from_le_bytes(bytes)
        }
    }

    fn read_u32(&mut self, big_endian: bool) -> u32 {
        let mut bytes: [u8; 4] = [0;4];
        self.read_exact(&mut bytes).unwrap();

        if big_endian {
            u32::from_be_bytes(bytes)
        } else {
            u32::from_le_bytes(bytes)
        }
    }

    fn read_u16(&mut self, big_endian: bool) -> u16 {
        let mut bytes: [u8; 2] = [0;2];
        self.read_exact(&mut bytes).unwrap();
        if big_endian {
            u16::from_be_bytes(bytes)
        } else {
            u16::from_le_bytes(bytes)
        }
    }

    fn read_u8(&mut self) -> u8 {
        let mut bytes: [u8; 1] = [0;1];
        self.read_exact(&mut bytes).unwrap();
        bytes[0]
    }
}

impl<T: Read> ByteReader for T {}

#[derive(Debug, PartialEq)]
pub struct ZoomLevel {
    reduction_level: u32,
    reserved: u32,
    data_offset: u64,
    index_offset: u64,
}

#[derive(Debug, PartialEq)]
pub struct FileOffsetSize{
    offset: usize,
    size: usize,
}

pub fn find_file_offset_gap(block_list: &[FileOffsetSize]) -> (&[FileOffsetSize], &[FileOffsetSize]) {
    for (index, block) in block_list.iter().enumerate() {
        let next = index + 1;
        // find the first gap
        if next < block_list.len()  && block_list[next].offset != block.offset + block.size {
            return (&block_list[..next], &block_list[next..])
        }
    }
    (&block_list[..], &[])
}

fn strip_null(inp: &str) -> &str {
    let mut start = 0;
    for (index, byte) in inp.bytes().enumerate() {
        if start == index && byte == 0 {
            start += 1
        } else {
            break
        }
    }
    for (index, byte) in inp.bytes().enumerate().skip(start) {
        if byte == 0 {
            return &inp[start..index]
        }
    }
    &inp[start..]
}

#[derive(Debug, PartialEq)]
pub struct Chrom{
    name: String,
    id: u32,
    size: u32,
}

#[derive(Debug, PartialEq)]
pub struct BedLine {
    chrom_id: u32,
    start: u32,
    end: u32,
    rest: Option<String>,
}

#[derive(Debug)]
struct BPlusTreeFile { 
    big_endian: bool,
    block_size: u32,
    key_size: usize,
    val_size: usize,
    item_count: u64,
    root_offset: u64,
}

impl BPlusTreeFile {
    fn with_reader<T: Read + Seek>(reader: &mut T) -> Result<BPlusTreeFile, Error> {
        // check the signature first
        let mut buff = [0; 4];
        reader.read_exact(&mut buff)?;
        let big_endian =
            if buff == BPT_SIG {
                true
            } else if buff.iter().eq(BPT_SIG.iter().rev()) {
                false
            } else {
                return Err(Error::BadSig{expected: BPT_SIG, received: buff});
            };

        //read all the header information
        let block_size = reader.read_u32(big_endian);
        let key_size = reader.read_u32(big_endian).try_into()?;
        let val_size = reader.read_u32(big_endian).try_into()?;
        let item_count = reader.read_u64(big_endian);

        // skip over the reserved region and get the root offset
        let root_offset = reader.seek(SeekFrom::Current(8))?;
        Ok(BPlusTreeFile{big_endian, block_size, key_size, val_size, item_count, root_offset})
    }

    //TODO: eventually abstract the traversal function as an iterator
    fn chrom_list<T: Read + Seek>(&self, reader: &mut T) -> Result<Vec<Chrom>, Error> {
        // move reader to the root_offset
        let mut chroms: Vec<Chrom> = Vec::new();
        let mut offsets = VecDeque::new();
        offsets.push_back(self.root_offset);
        while let Some(offset) = offsets.pop_back() {
            // move to the offset
            reader.seek(SeekFrom::Start(offset))?;
            
            // read block header
            let is_leaf = reader.read_u8();
            let _reserved = reader.read_u8();
            let child_count = reader.read_u16(self.big_endian);

            if is_leaf != 0 {
                let mut valbuf: Vec<u8> = vec![0; self.val_size.try_into().unwrap()];
                for _  in 0..child_count {
                    let mut keybuf: Vec<u8> = vec![0; self.key_size.try_into().unwrap()];
                    //TODO: move this into the declaration of the file
                    if self.val_size != 8 {
                        panic!("Expected chromosome data to be 8 bytes not, {}", self.val_size)
                    }
                    reader.read_exact(&mut keybuf)?;
                    reader.read_exact(&mut valbuf)?;
                    
                    let id = if self.big_endian {
                        u32::from_be_bytes(valbuf[0..4].try_into().unwrap())
                    } else {
                        u32::from_le_bytes(valbuf[0..4].try_into().unwrap())
                    };
                    let size = if self.big_endian {
                        u32::from_be_bytes(valbuf[4..8].try_into().unwrap())
                    } else {
                        u32::from_le_bytes(valbuf[4..8].try_into().unwrap())
                    };
                    chroms.push(Chrom{
                        name: String::from_utf8(keybuf).unwrap(), id, size
                    })
                }
            } else {
                for _ in 0..child_count {
                    // skip over the key in each block
                    // note that keysize is typically a few bytes, so this should not panic
                    reader.seek(SeekFrom::Current(self.key_size.try_into()?))?;
                    // read an offset and add it to the list to traverse
                    offsets.push_back(reader.read_u64(self.big_endian));
                }
            }
        }
        Ok(chroms)
    }

    // TODO: abstract this method
    fn find<T: Read + Seek>(&self, chrom: &str, reader: &mut T) -> Result<Option<Chrom>, Error> {
        if chrom.len() > self.key_size {
            return Err(Error::BadKey(chrom.to_owned(), self.key_size))
        }
        // if key is too short, we need to pad it with null character
        if chrom.len() != (self.key_size) {
            // prepare a new key
            let mut padded_key = String::with_capacity(self.key_size);
            padded_key.push_str(chrom);

            let needed: usize = self.key_size - chrom.len();
            for _ in 0..needed {
                padded_key.push('\0');
            }
            self._find_internal(&padded_key, reader)
        } else {
            self._find_internal(chrom, reader)
        }
    }

    fn _find_internal<T: Read + Seek>(&self, chrom: &str, reader: &mut T) -> Result<Option<Chrom>, Error> {
        let mut offsets = VecDeque::new();
        offsets.push_back(self.root_offset);
        while let Some(offset) = offsets.pop_back() {
            // move to the offset
            reader.seek(SeekFrom::Start(offset))?;

            // read block header
            let is_leaf = reader.read_u8();
            let _reserved = reader.read_u8();
            let child_count = reader.read_u16(self.big_endian);

            if is_leaf != 0 {
                let mut valbuf: Vec<u8> = vec![0; self.val_size.try_into().unwrap()];
                for _  in 0..child_count {
                    let mut keybuf: Vec<u8> = vec![0; self.key_size.try_into().unwrap()];
                    reader.read(&mut keybuf)?;
                    reader.read(&mut valbuf)?;
                    let other_key = String::from_utf8(keybuf).unwrap();
                    if other_key == chrom {
                        if self.val_size != 8 {
                            panic!("Expected chromosome data to be 8 bytes not, {}", self.val_size)
                        }
                        let id = if self.big_endian {
                            u32::from_be_bytes(valbuf[0..4].try_into().unwrap())
                        } else {
                            u32::from_le_bytes(valbuf[0..4].try_into().unwrap())
                        };
                        let size = if self.big_endian {
                            u32::from_be_bytes(valbuf[4..8].try_into().unwrap())
                        } else {
                            u32::from_le_bytes(valbuf[4..8].try_into().unwrap())
                        };
                        // return the proper data
                        return Ok(Some(Chrom{name: other_key, id, size}))
                    }
                }
            } else {
                // skip past the first key
                reader.seek(SeekFrom::Current(self.key_size.try_into()?))?;

                // read the offset
                let mut prev_offset = reader.read_u64(self.big_endian);
                for _ in 1..child_count {
                    let mut keybuf: Vec<u8> = vec![0; self.key_size];
                    reader.read(&mut keybuf)?;
                    let other_key = String::from_utf8(keybuf).unwrap();
                    // if find a bigger key, that means we passed our good key
                    if chrom < &other_key {
                        offsets.push_back(prev_offset);
                    }
                    // otherwise: read the next offset and keep going
                    prev_offset = reader.read_u64(self.big_endian);
                }
            }
        }
        Ok(None)
    }
}

#[derive(Debug)]
struct CIRTreeFile {
    big_endian: bool,
    block_size: u32,
    item_count: u64,
    start_chrom_ix: u32,
    start_base: u32,
    end_chrom_ix: u32,
    end_base: u32,
    file_size: u64,
    items_per_slot: u32,
    root_offset: u64,
}

fn cir_overlaps(q_chrom: u32, q_start: u32, q_end: u32, 
                start_chrom: u32, start_base: u32, 
                end_chrom: u32, end_base: u32) -> bool {
    (q_chrom, q_start) < (end_chrom, end_base) 
    && (q_chrom, q_end) > (start_chrom, start_base)
}

impl CIRTreeFile {
    fn with_reader<T: Read + Seek>(reader: &mut T) -> Result<CIRTreeFile, Error> {
        // check the signature first
        let mut buff = [0; 4];
        reader.read_exact(&mut buff)?;
        let big_endian =
            if buff == CIRTREE_SIG {
                true
            } else if buff.iter().eq(CIRTREE_SIG.iter().rev()) {
                false
            } else {
                return Err(Error::BadSig{expected: CIRTREE_SIG, received: buff});
            };

        //read all the header information
        let block_size = reader.read_u32(big_endian);
        let item_count = reader.read_u64(big_endian);
        let start_chrom_ix = reader.read_u32(big_endian);
        let start_base = reader.read_u32(big_endian);
        let end_chrom_ix = reader.read_u32(big_endian);
        let end_base = reader.read_u32(big_endian);
        let file_size = reader.read_u64(big_endian);
        let items_per_slot = reader.read_u32(big_endian);

        // skip over the reserved region and get the root offset
        let root_offset = reader.seek(SeekFrom::Current(4))?;

        Ok(CIRTreeFile{
            big_endian,
            block_size,
            item_count,
            start_chrom_ix,
            start_base,
            end_chrom_ix,
            end_base,
            file_size,
            items_per_slot,
            root_offset,
        })
    }

    fn find_blocks<T: Read + Seek>(&self, chrom_id: u32, start: u32, end: u32, reader: &mut T) -> Result<Vec<FileOffsetSize>, Error> {
        let mut blocks = Vec::<FileOffsetSize>::new();
        let mut offsets = VecDeque::new();
        offsets.push_back(self.root_offset);
        while let Some(offset) = offsets.pop_back() {
            // move to the offset
            reader.seek(SeekFrom::Start(offset))?;
            
            // read block header
            let is_leaf = reader.read_u8();
            let _reserved = reader.read_u8();
            let child_count = reader.read_u16(self.big_endian);
            //eprintln!("is_leaf {}", child_count);
            //eprintln!("child_count {}", child_count);

            if is_leaf != 0 {
                for _  in 0..child_count {
                    let start_chrom = reader.read_u32(self.big_endian);
                    let start_base = reader.read_u32(self.big_endian);
                    let end_chrom = reader.read_u32(self.big_endian);
                    let end_base = reader.read_u32(self.big_endian);
                    let offset = reader.read_u64(self.big_endian).try_into()?;
                    let size = reader.read_u64(self.big_endian).try_into()?;
                    //eprint!("chrom_id {}; start {}; end {}; start_chrom {}; start_base {}; end_chrom {}; end_base {};",
                    //          chrom_id, start, end, start_chrom, start_base, end_chrom, end_base);
                    if cir_overlaps(chrom_id, start, end, start_chrom, start_base, end_chrom, end_base) {
                        blocks.push(FileOffsetSize{offset, size})
                    }
                }
            } else {
                for _ in 0..child_count {
                    // load the data in the Node
                    let start_chrom = reader.read_u32(self.big_endian);
                    let start_base = reader.read_u32(self.big_endian);
                    let end_chrom = reader.read_u32(self.big_endian);
                    let end_base = reader.read_u32(self.big_endian);
                    let offset = reader.read_u64(self.big_endian);

                    // if we have overlaps in this area, then we should explore the node
                    //eprint!("chrom_id {}; start {}; end {}; start_chrom {}; start_base {}; end_chrom {}; end_base {};",
                    //         chrom_id, start, end, start_chrom, start_base, end_chrom, end_base);
                    if cir_overlaps(chrom_id, start, end, start_chrom, start_base, end_chrom, end_base) {
                        offsets.push_back(offset);
                    }
                }
            }
        }
        Ok(blocks)
    }
}

#[derive(Debug)]
pub struct BigBed<T: Read + Seek>  {
    reader: T,
    pub big_endian: bool,
    pub version: u16,
    pub zoom_levels: u16,
    pub chrom_tree_offset: u64,
    pub unzoomed_data_offset: u64,
    pub unzoomed_index_offset: u64,
    pub field_count: u16,
    pub defined_field_count: u16,
    pub as_offset: u64,
    pub total_summary_offset: u64,
    pub uncompress_buf_size: usize,
    pub extension_offset: u64,
    pub level_list: Vec<ZoomLevel>,
    pub extension_size: Option<u16>,
    pub extra_index_count: Option<u16>,
    pub extra_index_list_offset: Option<u64>,
    chrom_bpt: BPlusTreeFile,
    unzoomed_cir: Option<CIRTreeFile>,
}

impl<T: Read + Seek> BigBed<T> {
    pub fn from_file(mut reader: T) -> Result<BigBed<T>, Error> {
        let mut buff = [0; 4];
        reader.read_exact(&mut buff)?;
        let big_endian =
            if buff == BIGBED_SIG {
                true
            } else if buff.iter().eq(BIGBED_SIG.iter().rev()) {
                false
            } else {
                return Err(Error::BadSig{expected: BIGBED_SIG, received: buff});
            };
        let version = reader.read_u16(big_endian);
        let zoom_levels = reader.read_u16(big_endian);
        let chrom_tree_offset = reader.read_u64(big_endian);
        let unzoomed_data_offset = reader.read_u64(big_endian);
        let unzoomed_index_offset = reader.read_u64(big_endian);
        let field_count = reader.read_u16(big_endian);
        let defined_field_count = reader.read_u16(big_endian);
        let as_offset = reader.read_u64(big_endian);
        let total_summary_offset = reader.read_u64(big_endian);
        let uncompress_buf_size = reader.read_u32(big_endian).try_into()?;
        let extension_offset = reader.read_u64(big_endian);

        let mut level_list: Vec<ZoomLevel> = Vec::with_capacity(usize::from(zoom_levels));
        for _ in 0..usize::from(zoom_levels) {
            level_list.push(ZoomLevel{
                reduction_level: reader.read_u32(big_endian),
                reserved: reader.read_u32(big_endian),
                data_offset: reader.read_u64(big_endian),
                index_offset: reader.read_u64(big_endian)
            })
        }

        let mut extension_size = None;
        let mut extra_index_count = None;
        let mut extra_index_list_offset = None;

        if extension_offset != 0 {
            // move to extension
            reader.seek(SeekFrom::Start(extension_offset))?;
            extension_size = Some(reader.read_u16(big_endian));
            extra_index_count = Some(reader.read_u16(big_endian));
            extra_index_list_offset = Some(reader.read_u64(big_endian));
        }

        //move to the B+ tree file region
        reader.seek(SeekFrom::Start(chrom_tree_offset))?;
        let chrom_bpt = BPlusTreeFile::with_reader(&mut reader)?;

        Ok(BigBed{
            reader, big_endian, version, zoom_levels, chrom_tree_offset, 
            unzoomed_data_offset, unzoomed_index_offset, field_count,
            defined_field_count, as_offset, total_summary_offset, 
            uncompress_buf_size, extension_offset, level_list,
            extension_size, extra_index_count, extra_index_list_offset,
            chrom_bpt, unzoomed_cir: None,
        })
    }
    
    pub fn attach_unzoomed_cir(&mut self) -> Result<(), Error>{
        if self.unzoomed_cir.is_none() {
            // if not, seek to where the reader should be
            self.reader.seek(SeekFrom::Start(self.unzoomed_index_offset))?;
            // and attach the index (i.e. read the header)
            self.unzoomed_cir = Some(
                CIRTreeFile::with_reader(&mut self.reader)?
            );
        }
        Ok(())
    }
    
    pub fn overlapping_blocks(&mut self, chrom_id: u32, 
                          start: u32, end: u32) -> Result<Vec<FileOffsetSize>, Error> {
        
        // ensure that unzoomed_cir is attached
        self.attach_unzoomed_cir()?;
        // this operation is guaranteed to work now
        let index = self.unzoomed_cir.as_ref().unwrap();
        Ok(index.find_blocks(chrom_id, start, end, &mut self.reader)?)
    }
 
    pub fn query(&mut self, chrom: &str, start: u32, end: u32, max_items: u32) -> Result<Vec<BedLine>, Error> {
        let mut lines: Vec<BedLine> = Vec::new();
        let mut item_count: u32 = 0;

        let chrom_id: Option<u32>;
        // search for the chrom_id
        if let Some(chrom_data) = self.find_chrom(chrom)? {
            chrom_id = Some(chrom_data.id);
        // search for chrom_id without the 'chr'
        } else if let Some(chrom_data) = self.find_chrom(&chrom[3..])? {
            chrom_id = Some(chrom_data.id);
        } else {
            return Err(BadChrom(chrom.to_owned()));
        }
        // this operation is safe, otherwise the return above will be invoked
        let chrom_id = chrom_id.unwrap();
        // from kent:
        // "Find blocks with padded start and end to make sure we include zero-length insertions"
        let padded_start = if start > 0 {start - 1} else {start};
        let padded_end = end + 1;
        let blocks = self.overlapping_blocks(chrom_id, padded_start, padded_end)?;
        
        let mut decompressor = None;
        let mut decom_buff = None;
        if self.uncompress_buf_size > 0 {
            decompressor = Some(Decompress::new(true));
            decom_buff = Some(vec![0u8; self.uncompress_buf_size]);
        }

        let mut remaining = &blocks[..];
        while remaining.len() > 0 {
            // iterate through the list of blocks, get a slice of contiguous blocks
            let split = find_file_offset_gap(remaining);
            let before_gap = split.0;
            remaining = split.1;

            // get the offset
            let merged_offset = before_gap[0].offset;
            // get the total size
            // note: these unwraps are safe because we must have at least one element
            // (otherwise the loop would terminate)
            let merged_size = before_gap.last().unwrap().offset + before_gap.last().unwrap().size - merged_offset;
            // read in all the contigious blocks
            let mut merged_buff: Vec<u8> = vec![0; merged_size as usize];
            self.reader.seek(SeekFrom::Start(merged_offset.try_into()?))?;
            self.reader.read_exact(&mut merged_buff)?;
            
            
            // for each block in the merged group
            //eprintln!("{}", merged_buff.len());
            //eprintln!("{:?}", before_gap);
            for block in before_gap {
                let mut index: usize = 0;
                let block_start = block.offset - merged_offset;
                let mut block_end = block_start + block.size;
                let mut buff = &merged_buff[block_start..block_end];
                if self.uncompress_buf_size > 0 {
                    let debuff =  decom_buff.as_mut().unwrap();
                    let decomp =  decompressor.as_mut().unwrap();
                    //eprintln!("new block {} {}", block_start, block_end);
                    let status = decomp.decompress(&buff, debuff, FlushDecompress::Finish)?;
                    match status {
                        flate2::Status::Ok | flate2::Status::StreamEnd => {}
                        _ => {
                            eprintln!("{:?}", status);
                            return Err(Error::Misc("Decompression error!"));
                        }
                    }   

                    //eprintln!("total out {:?}", decomp.total_out());
                    block_end = decomp.total_out() as usize;
                    decomp.reset(true);
                    buff = &*debuff;
                }
                // iterate over the individual bytes in this block
                while index < block_end {
                    // read in chrom_id
                    let bytes: [u8; 4] = buff[index..index+4].try_into().expect("Failed to convert bytes");
                    let chr = if self.big_endian {u32::from_be_bytes(bytes)} else {u32::from_le_bytes(bytes)};
                    index += 4;
                    // read in start
                    let bytes: [u8; 4] = buff[index..index+4].try_into().expect("Failed to convert bytes");
                    let s = if self.big_endian {u32::from_be_bytes(bytes)} else {u32::from_le_bytes(bytes)};
                    index += 4;
                    // read in end
                    let bytes: [u8; 4] = buff[index..index+4].try_into().expect("Failed to convert bytes");
                    let e = if self.big_endian {u32::from_be_bytes(bytes)} else {u32::from_le_bytes(bytes)};
                    index += 4;

                    // calculate how much data is left (if any)
                    // find the next '\0' character
                    let mut rest_length = 0;
                    for (index, byte) in buff[index..block_end].iter().enumerate() {
                        if byte == &0 {
                            rest_length = index;
                            break;
                        }
                    }
                    //eprintln!("{} {} {} {}", chr, s, e, rest_length);
                    //eprintln!("{}, {}", index, rest_length + index);
                    // check if this data is in the correct range
                    if chr == chrom_id && ( (s < end && e > start) || (s == e && (s == end || end == start) )) {
                        item_count += 1;
                        if max_items > 0 && item_count > max_items {
                            break;
                        }
                        // get the rest of the data if it is present
                        let rest = if rest_length > 0 {
                            Some(String::from_utf8(buff[index..rest_length+index].to_vec()).expect("FUCK"))
                        } else {
                            None
                        };
                        // add the BedLine to the list
                        lines.push(BedLine{
                            chrom_id: chr,
                            start: s,
                            end: e,
                            rest
                        });
                    }
                    // rest_length + 1 will be at the null character
                    //eprintln!("pastloop");
                    index += rest_length + 1;
                }
                // propagate the break statement
                if max_items > 0 && item_count > max_items {
                    break;
                }
            }
            if max_items > 0 && item_count > max_items {
                break;
            }
        }
        Ok(lines)
    }

    pub fn to_bed(&mut self, chrom: Option<&str>, start: Option<u32>, end: Option<u32>, max_items: Option<u32>, mut output: impl Write) -> Result<(), Error> {
        let item_count = 0;
        for chrom_data in self.chrom_list()? {
            //TODO: check for null characters
            if let Some(name) = chrom {
                if name != strip_null(&chrom_data.name) {
                    continue
                }
            }
            let start = match start {
                None => 0,
                Some(value) => value,
            };
            let end = match end {
                None => chrom_data.size,
                Some(value) => value,
            };
            // check on the total number of items
            let mut items_left = 0;
            if let Some(max_value) = max_items {
                items_left = max_value - item_count;
                // stop iteration if we have exceeded the limit
                if items_left <= 0 {
                    break;
                }
            }

            let name_to_print = strip_null(&chrom_data.name);
            let interval_list = self.query(&chrom_data.name, start, end, items_left).unwrap();
            for bed_line in interval_list.into_iter() {
                match bed_line.rest {
                    None => {
                        output.write(format!("{}\t{}\t{}\n", name_to_print, bed_line.start, bed_line.end).as_bytes())?;
                    } Some(data) => {
                        output.write(format!("{}\t{}\t{}\t{}\n", name_to_print, bed_line.start, bed_line.end, data).as_bytes())?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn chrom_list(&mut self) -> Result<Vec<Chrom>, Error> {
        self.chrom_bpt.chrom_list(&mut self.reader)
    }

    pub fn find_chrom(&mut self, chrom: &str) -> Result<Option<Chrom>, Error> {
        self.chrom_bpt.find(chrom, &mut self.reader)
    }
}

#[cfg(test)]
mod test_bb {
    use std::fs::File;
    use super::*;

    //TODO: add testcase for nonexistent file
    fn bb_from_file(filename: &str) -> Result<BigBed<File>, Error> {
        BigBed::from_file(File::open(filename)?)
    }

    //test for file signatures
    #[test]
    fn from_file_not_bigbed() {
        // this produces a 'File I/O error because the file is empty (no bytes can be read)
        let result = bb_from_file("test/beds/empty.bed").unwrap_err();
        if let Error::IOError(_) = result {
            // do a more manual check?
        } else {
            panic!("Expected IOError, received {:?}", result)
        }
        let result = bb_from_file("test/beds/one.bed").unwrap_err();
        assert_eq!(result, Error::BadSig{expected: BIGBED_SIG, received: [99, 104, 114, 55]});
        let result = bb_from_file("test/notbed.png").unwrap_err();
        assert_eq!(result, Error::BadSig{expected: BIGBED_SIG, received: [137, 80, 78, 71]});
    }

    //test a bigbed made from a one-line bed file
    #[test]
    fn from_file_onebed() {
        let bb = bb_from_file("test/bigbeds/one.bb").unwrap();
        assert_eq!(bb.as_offset, 304);
        assert_eq!(bb.chrom_tree_offset, 628);
        assert_eq!(bb.defined_field_count, 3);
        assert_eq!(bb.extension_offset, 564);
        assert_eq!(bb.extension_size, Some(64));
        assert_eq!(bb.extra_index_count, Some(0));
        assert_eq!(bb.extra_index_list_offset, Some(0));
        assert_eq!(bb.field_count, 3);
        assert_eq!(bb.big_endian, false);
        assert_eq!(bb.total_summary_offset, 524);
        assert_eq!(bb.uncompress_buf_size, 16384);
        assert!(bb.unzoomed_cir.is_none());
        assert_eq!(bb.unzoomed_data_offset, 676);
        assert_eq!(bb.unzoomed_index_offset, 700);
        assert_eq!(bb.version, 4);
        assert_eq!(bb.zoom_levels, 1);
        assert_eq!(bb.level_list, vec![
            ZoomLevel{reduction_level: 107485656, reserved: 0, data_offset: 6904, index_offset: 6936}
        ])
    }

    #[test]
    fn from_file_longbed() {
        let bb = bb_from_file("test/bigbeds/long.bb").unwrap();
        assert_eq!(bb.as_offset, 304);
        assert_eq!(bb.chrom_tree_offset, 628);
        assert_eq!(bb.defined_field_count, 3);
        assert_eq!(bb.extension_offset, 564);
        assert_eq!(bb.extension_size, Some(64));
        assert_eq!(bb.extra_index_count, Some(0));
        assert_eq!(bb.extra_index_list_offset, Some(0));
        assert_eq!(bb.field_count, 3);
        assert_eq!(bb.big_endian, false);
        assert_eq!(bb.total_summary_offset, 524);
        assert_eq!(bb.uncompress_buf_size, 16384);
        assert!(bb.unzoomed_cir.is_none());
        assert_eq!(bb.unzoomed_data_offset, 976);
        assert_eq!(bb.unzoomed_index_offset, 80369);
        assert_eq!(bb.version, 4);
        assert_eq!(bb.zoom_levels, 5);
        assert_eq!(bb.level_list, vec![
                    ZoomLevel{reduction_level: 2440976, reserved: 0, data_offset: 86757, index_offset: 106847},
                    ZoomLevel{reduction_level: 9763904, reserved: 0, data_offset: 113067, index_offset: 119611},
                    ZoomLevel{reduction_level: 39055616, reserved: 0, data_offset: 125815, index_offset: 127568},
                    ZoomLevel{reduction_level: 156222464, reserved: 0, data_offset: 133772, index_offset: 134387},
                    ZoomLevel{reduction_level: 624889856, reserved: 0, data_offset: 140591, index_offset: 141086}
        ]);
    }

    #[test]
    fn test_chrom_list() {
        let mut bb = bb_from_file("test/bigbeds/one.bb").unwrap();
        // should only include the chromosomes mapped in the file
        assert_eq!(bb.chrom_list().unwrap(), vec![Chrom{name: String::from("chr7"), id: 0, size: 159345973}]);
        // same list should be generated a second time
        assert_eq!(bb.chrom_list().unwrap(), vec![Chrom{name: String::from("chr7"), id: 0, size: 159345973}]);
        // should include all chromosomes
        let mut bb = bb_from_file("test/bigbeds/long.bb").unwrap();
        assert_eq!(bb.chrom_list().unwrap(), vec![
            Chrom{name: String::from("chr1\0"), id: 0, size: 248956422},
            Chrom{name: String::from("chr10"), id: 1, size: 133797422},
            Chrom{name: String::from("chr11"), id: 2, size: 135086622},
            Chrom{name: String::from("chr12"), id: 3, size: 133275309},
            Chrom{name: String::from("chr13"), id: 4, size: 114364328},
            Chrom{name: String::from("chr14"), id: 5, size: 107043718},
            Chrom{name: String::from("chr15"), id: 6, size: 101991189},
            Chrom{name: String::from("chr16"), id: 7, size: 90338345},
            Chrom{name: String::from("chr17"), id: 8, size: 83257441},
            Chrom{name: String::from("chr18"), id: 9, size: 80373285},
            Chrom{name: String::from("chr19"), id: 10, size: 58617616},
            Chrom{name: String::from("chr2\0"), id: 11, size: 242193529},
            Chrom{name: String::from("chr20"), id: 12, size: 64444167},
            Chrom{name: String::from("chr21"), id: 13, size: 46709983},
            Chrom{name: String::from("chr22"), id: 14, size: 50818468},
            Chrom{name: String::from("chr3\0"), id: 15, size: 198295559},
            Chrom{name: String::from("chr4\0"), id: 16, size: 190214555},
            Chrom{name: String::from("chr5\0"), id: 17, size: 181538259},
            Chrom{name: String::from("chr6\0"), id: 18, size: 170805979},
            Chrom{name: String::from("chr7\0"), id: 19, size: 159345973},
            Chrom{name: String::from("chr8\0"), id: 20, size: 145138636},
            Chrom{name: String::from("chr9\0"), id: 21, size: 138394717},
            Chrom{name: String::from("chrX\0"), id: 22, size: 156040895},
            Chrom{name: String::from("chrY\0"), id: 23, size: 57227415}
        ]);
        let mut bb = bb_from_file("test/bigbeds/tair10-nochr.bb").unwrap();
        assert_eq!(bb.chrom_list().unwrap(), vec![
            Chrom{name: String::from("1"), id: 0, size: 30427671},
            Chrom{name: String::from("2"), id: 1, size: 19698289},
            Chrom{name: String::from("3"), id: 2, size: 23459830},
            Chrom{name: String::from("4"), id: 3, size: 18585056},
            Chrom{name: String::from("5"), id: 4, size: 26975502},
            Chrom{name: String::from("C"), id: 5, size: 154478},
            Chrom{name: String::from("M"), id: 6, size: 366924}
        ]);
        let mut bb = bb_from_file("test/bigbeds/tair10.bb").unwrap();
        assert_eq!(bb.chrom_list().unwrap(), vec![
            Chrom{name: String::from("Chr1"), id: 0, size: 30427671},
            Chrom{name: String::from("Chr2"), id: 1, size: 19698289},
            Chrom{name: String::from("Chr3"), id: 2, size: 23459830},
            Chrom{name: String::from("Chr4"), id: 3, size: 18585056},
            Chrom{name: String::from("Chr5"), id: 4, size: 26975502},
            Chrom{name: String::from("ChrC"), id: 5, size: 154478},
            Chrom{name: String::from("ChrM"), id: 6, size: 366924}
        ]);
        // testing with an extremely large chrom.sizes file:
        let mut bb = bb_from_file("test/bigbeds/mm10.bb").unwrap();
        assert_eq!(bb.chrom_list().unwrap(), vec![
            Chrom{name: String::from("chr1\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 0, size: 195471971},
            Chrom{name: String::from("chr10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 1, size: 130694993},
            Chrom{name: String::from("chr11\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 2, size: 122082543},
            Chrom{name: String::from("chr12\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 3, size: 120129022},
            Chrom{name: String::from("chr13\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 4, size: 120421639},
            Chrom{name: String::from("chr14\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 5, size: 124902244},
            Chrom{name: String::from("chr15\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 6, size: 104043685},
            Chrom{name: String::from("chr16\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 7, size: 98207768},
            Chrom{name: String::from("chr17\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 8, size: 94987271},
            Chrom{name: String::from("chr18\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 9, size: 90702639},
            Chrom{name: String::from("chr19\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 10, size: 61431566},
            Chrom{name: String::from("chr1_GL456210_random"), id: 11, size: 169725},
            Chrom{name: String::from("chr1_GL456211_random"), id: 12, size: 241735},
            Chrom{name: String::from("chr1_GL456212_random"), id: 13, size: 153618},
            Chrom{name: String::from("chr1_GL456213_random"), id: 14, size: 39340},
            Chrom{name: String::from("chr1_GL456221_random"), id: 15, size: 206961},
            Chrom{name: String::from("chr2\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 16, size: 182113224},
            Chrom{name: String::from("chr3\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 17, size: 160039680},
            Chrom{name: String::from("chr4\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 18, size: 156508116},
            Chrom{name: String::from("chr4_GL456216_random"), id: 19, size: 66673},
            Chrom{name: String::from("chr4_GL456350_random"), id: 20, size: 227966},
            Chrom{name: String::from("chr4_JH584292_random"), id: 21, size: 14945},
            Chrom{name: String::from("chr4_JH584293_random"), id: 22, size: 207968},
            Chrom{name: String::from("chr4_JH584294_random"), id: 23, size: 191905},
            Chrom{name: String::from("chr4_JH584295_random"), id: 24, size: 1976},
            Chrom{name: String::from("chr5\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 25, size: 151834684},
            Chrom{name: String::from("chr5_GL456354_random"), id: 26, size: 195993},
            Chrom{name: String::from("chr5_JH584296_random"), id: 27, size: 199368},
            Chrom{name: String::from("chr5_JH584297_random"), id: 28, size: 205776},
            Chrom{name: String::from("chr5_JH584298_random"), id: 29, size: 184189},
            Chrom{name: String::from("chr5_JH584299_random"), id: 30, size: 953012},
            Chrom{name: String::from("chr6\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 31, size: 149736546},
            Chrom{name: String::from("chr7\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 32, size: 145441459},
            Chrom{name: String::from("chr7_GL456219_random"), id: 33, size: 175968},
            Chrom{name: String::from("chr8\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 34, size: 129401213},
            Chrom{name: String::from("chr9\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 35, size: 124595110},
            Chrom{name: String::from("chrM\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 36, size: 16299},
            Chrom{name: String::from("chrUn_GL456239\0\0\0\0\0\0"), id: 37, size: 40056},
            Chrom{name: String::from("chrUn_GL456359\0\0\0\0\0\0"), id: 38, size: 22974},
            Chrom{name: String::from("chrUn_GL456360\0\0\0\0\0\0"), id: 39, size: 31704},
            Chrom{name: String::from("chrUn_GL456366\0\0\0\0\0\0"), id: 40, size: 47073},
            Chrom{name: String::from("chrUn_GL456367\0\0\0\0\0\0"), id: 41, size: 42057},
            Chrom{name: String::from("chrUn_GL456368\0\0\0\0\0\0"), id: 42, size: 20208},
            Chrom{name: String::from("chrUn_GL456370\0\0\0\0\0\0"), id: 43, size: 26764},
            Chrom{name: String::from("chrUn_GL456372\0\0\0\0\0\0"), id: 44, size: 28664},
            Chrom{name: String::from("chrUn_GL456378\0\0\0\0\0\0"), id: 45, size: 31602},
            Chrom{name: String::from("chrUn_GL456379\0\0\0\0\0\0"), id: 46, size: 72385},
            Chrom{name: String::from("chrUn_GL456381\0\0\0\0\0\0"), id: 47, size: 25871},
            Chrom{name: String::from("chrUn_GL456382\0\0\0\0\0\0"), id: 48, size: 23158},
            Chrom{name: String::from("chrUn_GL456383\0\0\0\0\0\0"), id: 49, size: 38659},
            Chrom{name: String::from("chrUn_GL456385\0\0\0\0\0\0"), id: 50, size: 35240},
            Chrom{name: String::from("chrUn_GL456387\0\0\0\0\0\0"), id: 51, size: 24685},
            Chrom{name: String::from("chrUn_GL456389\0\0\0\0\0\0"), id: 52, size: 28772},
            Chrom{name: String::from("chrUn_GL456390\0\0\0\0\0\0"), id: 53, size: 24668},
            Chrom{name: String::from("chrUn_GL456392\0\0\0\0\0\0"), id: 54, size: 23629},
            Chrom{name: String::from("chrUn_GL456393\0\0\0\0\0\0"), id: 55, size: 55711},
            Chrom{name: String::from("chrUn_GL456394\0\0\0\0\0\0"), id: 56, size: 24323},
            Chrom{name: String::from("chrUn_GL456396\0\0\0\0\0\0"), id: 57, size: 21240},
            Chrom{name: String::from("chrUn_JH584304\0\0\0\0\0\0"), id: 58, size: 114452},
            Chrom{name: String::from("chrX\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 59, size: 171031299},
            Chrom{name: String::from("chrX_GL456233_random"), id: 60, size: 336933},
            Chrom{name: String::from("chrY\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"), id: 61, size: 91744698},
            Chrom{name: String::from("chrY_JH584300_random"), id: 62, size: 182347},
            Chrom{name: String::from("chrY_JH584301_random"), id: 63, size: 259875},
            Chrom{name: String::from("chrY_JH584302_random"), id: 64, size: 155838},
            Chrom{name: String::from("chrY_JH584303_random"), id: 65, size: 158099}
        ]);
    }
    
    #[test]
    fn test_find_chrom_one() {
         let mut bb = bb_from_file("test/bigbeds/one.bb").unwrap();
         assert_eq!(bb.find_chrom("chr1").unwrap(), None);
         assert_eq!(bb.find_chrom("chr7").unwrap(), Some(Chrom{name: String::from("chr7"), id: 0, size: 159345973}));
         // does it work again?
         assert_eq!(bb.find_chrom("chr7").unwrap(), Some(Chrom{name: String::from("chr7"), id: 0, size: 159345973}));
         assert_eq!(bb.find_chrom("chr").unwrap(), None);
         // key too long
         assert_eq!(bb.find_chrom("chr79"), Err(Error::BadKey(String::from("chr79"), 4)));
         // should be case-sensitive
         assert_eq!(bb.find_chrom("cHr7").unwrap(), None);
         // near-matches don't count
         assert_eq!(bb.find_chrom("xhr7").unwrap(), None);
    }

    #[test]
    fn test_find_chrom_long() {
        let mut bb = bb_from_file("test/bigbeds/long.bb").unwrap();
        assert_eq!(bb.find_chrom("chr2\0").unwrap(), Some(Chrom{name: String::from("chr2\0"), id: 11, size: 242193529}));
        // should work without padding
        assert_eq!(bb.find_chrom("chr2").unwrap(), Some(Chrom{name: String::from("chr2\0"), id: 11, size: 242193529}));
        // cannot omit the 'chr'
        assert_eq!(bb.find_chrom("2").unwrap(), None);
        // still should have key too long errors
        assert_eq!(bb.find_chrom("chr2xx"), Err(Error::BadKey(String::from("chr2xx"), 5)));
    }

    #[test]
    fn test_overlapping_blocks() {
        let mut bb = bb_from_file("test/bigbeds/long.bb").unwrap();
        assert_eq!(bb.overlapping_blocks(0, 100, 1000000), Ok(vec![FileOffsetSize{offset: 984, size: 3324}]));
        // swapped start and stop positions should produce no blocks
        assert_eq!(bb.overlapping_blocks(0, 100000, 10), Ok(vec![]));
        // trying a more narrow range
        assert_eq!(bb.overlapping_blocks(20, 131366255, 132257727), Ok(vec![FileOffsetSize{offset: 67045, size: 3295}]));
        // bad chromosome should just produce no blocks
        assert_eq!(bb.overlapping_blocks(42, 100000, 10), Ok(vec![]));
    }
}