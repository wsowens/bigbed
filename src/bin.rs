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

#[derive(Debug, PartialEq)]
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
    fn chrom_list(&self, reader: &mut File) -> Vec<Chrom> {
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
        eprintln!("running _find_internal {}", chrom);
        let mut offsets = VecDeque::new();
        offsets.push_back(self.root_offset);
        while let Some(offset) = offsets.pop_back() {
            // move to the offset
            reader.seek(SeekFrom::Start(offset));

            // read block header
            let is_leaf = reader.read_u8();
            let reserved = reader.read_u8();
            let child_count = reader.read_u16(self.big_endian);

            if is_leaf != 0 {
                let mut valbuf: Vec<u8> = vec![0; self.val_size.try_into().unwrap()];
                for _  in 0..child_count {
                    let mut keybuf: Vec<u8> = vec![0; self.key_size.try_into().unwrap()];
                    reader.read(&mut keybuf);
                    reader.read(&mut valbuf);
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

    fn chrom_list(&mut self) -> Vec<Chrom> {
        self.chrom_bpt.chrom_list(&mut self.reader)
    }

    fn find_chrom(&mut self, chrom: &str) -> Result<Option<Chrom>, &'static str> {
        self.chrom_bpt.find(chrom, &mut self.reader)
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
        ]);
    }

    #[test]
    fn test_chrom_list() {
        let mut bb = BigBed::from_file("test/bigbeds/one.bb").unwrap();
        // should only include the chromosomes mapped in the file
        assert_eq!(bb.chrom_list(), vec![Chrom{name: String::from("chr7"), id: 0, size: 159345973}]);
        // same list should be generated a second time
        assert_eq!(bb.chrom_list(), vec![Chrom{name: String::from("chr7"), id: 0, size: 159345973}]);
        // should include all chromosomes
        let mut bb = BigBed::from_file("test/bigbeds/long.bb").unwrap();
        assert_eq!(bb.chrom_list(), vec![
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
        let mut bb = BigBed::from_file("test/bigbeds/tair10-nochr.bb").unwrap();
        assert_eq!(bb.chrom_list(), vec![
            Chrom{name: String::from("1"), id: 0, size: 30427671},
            Chrom{name: String::from("2"), id: 1, size: 19698289},
            Chrom{name: String::from("3"), id: 2, size: 23459830},
            Chrom{name: String::from("4"), id: 3, size: 18585056},
            Chrom{name: String::from("5"), id: 4, size: 26975502},
            Chrom{name: String::from("C"), id: 5, size: 154478},
            Chrom{name: String::from("M"), id: 6, size: 366924}
        ]);
        let mut bb = BigBed::from_file("test/bigbeds/tair10.bb").unwrap();
        assert_eq!(bb.chrom_list(), vec![
            Chrom{name: String::from("Chr1"), id: 0, size: 30427671},
            Chrom{name: String::from("Chr2"), id: 1, size: 19698289},
            Chrom{name: String::from("Chr3"), id: 2, size: 23459830},
            Chrom{name: String::from("Chr4"), id: 3, size: 18585056},
            Chrom{name: String::from("Chr5"), id: 4, size: 26975502},
            Chrom{name: String::from("ChrC"), id: 5, size: 154478},
            Chrom{name: String::from("ChrM"), id: 6, size: 366924}
        ]);
        // testing with an extremely large chrom.sizes file:
        let mut bb = BigBed::from_file("test/bigbeds/mm10.bb").unwrap();
        assert_eq!(bb.chrom_list(), vec![
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
         let mut bb = BigBed::from_file("test/bigbeds/one.bb").unwrap();
         assert_eq!(bb.find_chrom("chr1").unwrap(), None);
         assert_eq!(bb.find_chrom("chr7").unwrap(), Some(Chrom{name: String::from("chr7"), id: 0, size: 159345973}));
         // does it work again?
         assert_eq!(bb.find_chrom("chr7").unwrap(), Some(Chrom{name: String::from("chr7"), id: 0, size: 159345973}));
         assert_eq!(bb.find_chrom("chr").unwrap(), None);
         // key too long
         assert_eq!(bb.find_chrom("chr79"), Err("Key too long."));
         // should be case-sensitive
         assert_eq!(bb.find_chrom("cHr7").unwrap(), None);
         // near-matches don't count
         assert_eq!(bb.find_chrom("xhr7").unwrap(), None);
    }

    #[test]
    fn test_find_chrom_long() {
        let mut bb = BigBed::from_file("test/bigbeds/long.bb").unwrap();
        assert_eq!(bb.find_chrom("chr2\0").unwrap(), Some(Chrom{name: String::from("chr2\0"), id: 11, size: 242193529}));
        // should work without padding
        assert_eq!(bb.find_chrom("chr2").unwrap(), Some(Chrom{name: String::from("chr2\0"), id: 11, size: 242193529}));
        // cannot omit the 'chr'
        assert_eq!(bb.find_chrom("2").unwrap(), None);
        // still should have key too long errors
        assert_eq!(bb.find_chrom("chr2xx"), Err("Key too long."));
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
            //println!("{:#?}", bb);
            println!("{:#?}", bb.find_chrom("chr7"));
        }
        Err(msg) => {
            eprintln!("{}", msg);
        }
    }
}