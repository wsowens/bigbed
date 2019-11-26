use std::fs::File;
use std::io::{Read, Seek};
use std::io::SeekFrom;
use std::collections::VecDeque;
use std::convert::TryInto;

static BIGBED_SIG: [u8; 4] = [0x87, 0x89, 0xF2, 0xEB];
static BPT_SIG: [u8; 4] = [0x78, 0xCA, 0x8C, 0x91];
static CIRTREE_SIG: [u8; 4] = [0x24, 0x68, 0xAC, 0xE0];

trait ByteReader: std::io::Read {
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

impl ByteReader for File {}

// a trait to ease with error propagation
trait Propagate<T> {
    fn propagate(self) -> Result<T, &'static str>;
}

impl<T> Propagate<T> for std::io::Result<T> {
    fn propagate(self) -> Result<T, &'static str> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                eprintln!("{}", x);
                Err("File I/O error")
            }
        }
    }
}

#[derive(Debug, PartialEq)]
struct ZoomLevel {
    reduction_level: u32,
    reserved: u32,
    data_offset: u64,
    index_offset: u64,
}

struct FileOffsetSize{
    offset: u64,
    size: u64,
}

#[derive(Debug)]
struct Chrom{
    name: String,
    id: u32,
    size: u32,
}

struct BedLine {
    chrom_id: u32,
    start: u32,
    end: u32,
    rest: Option<String>,
}

#[derive(Debug)]
struct BPlusTreeFile { 
    big_endian: bool,
    block_size: u32,
    key_size: u32,
    val_size: u32,
    item_count: u64,
    root_offset: u64,
}

impl BPlusTreeFile {
    fn with_reader(reader: &mut File) -> Result<BPlusTreeFile, &'static str> {
        // check the signature first
        let mut buff = [0; 4];
        reader.read_exact(&mut buff).propagate()?;
        let big_endian =
            if buff == BPT_SIG {
                true
            } else if buff.iter().eq(BPT_SIG.iter().rev()) {
                false
            } else {
                return Err("This is not a BPT file!");
            };

        //read all the header information
        let block_size = reader.read_u32(big_endian);
        let key_size = reader.read_u32(big_endian);
        let val_size = reader.read_u32(big_endian);
        let item_count = reader.read_u64(big_endian);

        // skip over the reserved region and get the root offset
        let root_offset = reader.seek(SeekFrom::Current(8)).propagate()?;
        Ok(BPlusTreeFile{big_endian, block_size, key_size, val_size, item_count, root_offset})
    }


    //TODO: eventually abstract the traversal function as an iterator
    fn chrom_list(self, reader: &mut File) -> Vec<Chrom> {
        // move reader to the root_offset
        let mut chroms: Vec<Chrom> = Vec::new();
        let mut offsets = VecDeque::new();
        offsets.push_back(self.root_offset);
        while let Some(offset) = offsets.pop_back() {
            // move to the offset
            println!("{}", reader.seek(SeekFrom::Start(offset)).unwrap());
            
            // read block header
            let is_leaf = reader.read_u8();
            let reserved = reader.read_u8();
            let child_count = reader.read_u16(self.big_endian);

            if is_leaf != 0 {
                let mut valbuf: Vec<u8> = vec![0; self.val_size.try_into().unwrap()];
                for _  in 0..child_count {
                    let mut keybuf: Vec<u8> = vec![0; self.key_size.try_into().unwrap()];
                    if self.val_size != 8 {
                        panic!("Expected chromosome data to be 8 bytes not, {}", self.val_size)
                    }
                    println!("read: {:?}", reader.read(&mut keybuf));
                    println!("read: {:?}", reader.read(&mut valbuf));
                    println!("{:?}", keybuf);
                    println!("{:?}", valbuf);
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
                    reader.seek(SeekFrom::Current(self.key_size.try_into().unwrap()));
                    // read an offset and add it to the list to traverse
                    offsets.push_back(reader.read_u64(self.big_endian));
                }
            }
        }
        chroms
    }

    // TODO: abstract this method
    fn find(&self, chrom: &str, reader: &mut File) -> Result<Option<Chrom>, &'static str> {
        if chrom.len() > self.key_size.try_into().unwrap() {
            return Err("Key too long.")
        }
        // if key is too short, we need to pad it with null character
        if chrom.len() != (self.key_size as usize) {
            // prepare a new key
            let mut padded_key = String::with_capacity(self.key_size.try_into().unwrap());
            padded_key.push_str(chrom);

            let needed: usize = (self.key_size as usize) - chrom.len();
            for _ in 0..needed {
                padded_key.push('\0');
            }
            self._find_internal(&padded_key, reader)
        } else {
            self._find_internal(chrom, reader)
        }
    }

    fn _find_internal(&self, chrom: &str, reader: &mut File) -> Result<Option<Chrom>, &'static str> {
        let mut offsets = VecDeque::new();
        offsets.push_back(self.root_offset);
        while let Some(offset) = offsets.pop_back() {
            // move to the offset
            
            // read block header
            let is_leaf = reader.read_u8();
            let reserved = reader.read_u8();
            let child_count = reader.read_u16(self.big_endian);

            if is_leaf != 0 {
                let mut valbuf: Vec<u8> = vec![0; self.val_size.try_into().unwrap()];
                for _  in 0..child_count {
                    let mut keybuf: Vec<u8> = vec![0; self.key_size.try_into().unwrap()];
                    reader.read(&mut keybuf);
                    let other_key = String::from_utf8(keybuf).unwrap();
                    if other_key == chrom {
                        if self.val_size != 8 {
                            panic!("Expected chromosome data to be 8 bytes not, {}", self.val_size)
                        }
                        reader.read(&mut valbuf);
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
                reader.seek(SeekFrom::Current(self.key_size.try_into().unwrap()));

                // read the offset
                let mut prev_offset = reader.read_u64(self.big_endian);
                for _ in 1..child_count {
                    let mut keybuf: Vec<u8> = vec![0; self.key_size.try_into().unwrap()];
                    reader.read(&mut keybuf);
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
    block_size: u32,
    item_count: u64,
    start_chrom_ix: u32,
    start_base: u32,
    end_chrom_ix: u32,
    end_base: u32,
    file_size: u64,
    items_per_slot: u32,
}

impl CIRTreeFile {
    fn with_reader(reader: &mut File) -> Result<CIRTreeFile, &'static str> {
        // check the signature first
        let mut buff = [0; 4];
        reader.read_exact(&mut buff).propagate()?;
        let big_endian =
            if buff == CIRTREE_SIG {
                true
            } else if buff.iter().eq(CIRTREE_SIG.iter().rev()) {
                false
            } else {
                return Err("This is not a CIRTree file!");
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
        let root_offset = reader.seek(SeekFrom::Current(4)).propagate()?;

        Ok(CIRTreeFile{
            block_size,
            item_count,
            start_chrom_ix,
            start_base,
            end_chrom_ix,
            end_base,
            file_size,
            items_per_slot,
        })
    }
}

#[derive(Debug)]
struct BigBed {
    reader: File,
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
    pub uncompress_buf_size: u32,
    pub extension_offset: u64,
    pub level_list: Vec<ZoomLevel>,
    pub extension_size: Option<u16>,
    pub extra_index_count: Option<u16>,
    pub extra_index_list_offset: Option<u64>,
    chrom_bpt: BPlusTreeFile,
    unzoomed_cir: Option<CIRTreeFile>,
}

impl BigBed {
    fn from_file(filename: &str) -> Result<BigBed, &'static str> {
        let mut reader = File::open(filename).propagate()?;
        let mut buff = [0; 4];
        reader.read_exact(&mut buff).propagate()?;
        let big_endian =
            if buff == BIGBED_SIG {
                true
            } else if buff.iter().eq(BIGBED_SIG.iter().rev()) {
                false
            } else {
                return Err("This is not a bigbed file!");
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
        let uncompress_buf_size = reader.read_u32(big_endian);
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
            reader.seek(SeekFrom::Start(extension_offset)).propagate()?;
            extension_size = Some(reader.read_u16(big_endian));
            extra_index_count = Some(reader.read_u16(big_endian));
            extra_index_list_offset = Some(reader.read_u64(big_endian));
        }

        //move to the B+ tree file region
        reader.seek(SeekFrom::Start(chrom_tree_offset)).propagate()?;
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
    /*
    fn overlapping_blocks(&self, index: &CIRTreeFile, chrom: Vec<u8>, 
                          start: u32, stop: u32, max_items: u32)  -> Result<Vec<FileOffsetSize>, &'static str> {
        // find the chrom data in the B+ tree
        chrom_data = match self.chrom_bpt.find(&chrom) {
            None => {
                // try without the 'chr'

            } Some(chrom) => {

            }
        }

    }*/

    fn query(&mut self, start: u32, end: u32, items: u32) -> Result<Vec<BedLine>, &'static str> {
        let lines: Vec<BedLine> = Vec::new();
        // check if the unzoomed index is attached
        if self.unzoomed_cir.is_none() {
            // if not, seek to where the reader should be
            self.reader.seek(SeekFrom::Start(self.unzoomed_index_offset));
            // and attach the index (i.e. read the header)
            self.unzoomed_cir = Some(
                CIRTreeFile::with_reader(&mut self.reader)?
            );
        }
        // this will never fail, because we just set the reader

        // from kent:
        // "Find blocks with padded start and end to make sure we include zero-length insertions"
        let paddedStart = if start > 0 {start - 1} else {start};
        let paddedEnd = end + 1;

        Ok(lines)
    }

}

#[cfg(test)]
mod test_bb {
    use super::*;

    //TODO: add testcase for nonexistent file

    //test for file signatures
    #[test]
    fn from_file_not_bigbed() {
        // this produces a 'File I/O error because the file is empty (no bytes can be read)
        let result = BigBed::from_file("test/beds/empty.bed").unwrap_err();
        assert_eq!(result, "File I/O error");
        let result = BigBed::from_file("test/beds/one.bed").unwrap_err();
        assert_eq!(result, "This is not a bigbed file!");
        let result = BigBed::from_file("test/notbed.png").unwrap_err();
        assert_eq!(result, "This is not a bigbed file!");
    }

    //test a bigbed made from a one-line bed file
    #[test]
    fn from_file_onebed() {
        let bb = BigBed::from_file("test/bigbeds/one.bb").unwrap();
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
        let bb = BigBed::from_file("test/bigbeds/long.bb").unwrap();
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
        ])
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Error: Please provide a filename!");
        std::process::exit(1);
    }
    match BigBed::from_file(&args[1]) {
        Ok(mut bb) => {
            println!("{:#?}", bb);
            println!("{:#?}", bb.chrom_bpt.chrom_list(&mut bb.reader));
        }
        Err(msg) => {
            eprintln!("{}", msg);
        }
    }
}