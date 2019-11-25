use std::fs::File;
use std::io::Read;
use std::io::BufReader;

static BIGBED_SIG: [u8; 4] = [0x87, 0x89, 0xF2, 0xEB];

trait ByteReader: std::io::Read {
    fn read_u64(&mut self, big_endian: bool) -> u64 {
        let mut bytes: [u8; 8] = [0;8];
        self.read_exact(&mut bytes);

        if big_endian {
            u64::from_be_bytes(bytes)
        } else {
            u64::from_le_bytes(bytes)
        }
    }

    fn read_u32(&mut self, big_endian: bool) -> u32 {
        let mut bytes: [u8; 4] = [0;4];
        self.read_exact(&mut bytes);

        if big_endian {
            u32::from_be_bytes(bytes)
        } else {
            u32::from_le_bytes(bytes)
        }
    }

    fn read_u16(&mut self, big_endian: bool) -> u16 {
        let mut bytes: [u8; 2] = [0;2];
        self.read_exact(&mut bytes);
        if big_endian {
            u16::from_be_bytes(bytes)
        } else {
            u16::from_le_bytes(bytes)
        }
    }

    fn read_u8(&mut self) -> u8 {
        let mut bytes: [u8; 1] = [0;1];
        self.read_exact(&mut bytes);
        bytes[0]
    }
}

impl ByteReader for File {}

#[derive(Debug)]
struct ZoomLevel {
    reduction_level: u32,
    reserved: u32,
    data_offset: u64,
    index_offset: u64,
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
}


impl BigBed {

    fn from_file(filename: &str) -> Result<BigBed, &'static str> {
        let mut reader: File = File::open(filename).unwrap();
        //let mut reader = BufReader::new(file);
        let mut buff = [0; 4];
        reader.read_exact(&mut buff);
        let big_endian =
            if buff == BIGBED_SIG {
                true
            } else if buff.iter().eq(BIGBED_SIG.iter().rev()) {
                false
            } else {
                return Err("This is not a bigbed file!")
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
        for i in 0..usize::from(zoom_levels) {
            level_list.push(ZoomLevel{
                reduction_level: reader.read_u32(big_endian),
                reserved: reader.read_u32(big_endian),
                data_offset: reader.read_u64(big_endian),
                index_offset: reader.read_u64(big_endian)
            })
        }

        Ok(BigBed{reader, big_endian, version, zoom_levels, chrom_tree_offset, unzoomed_data_offset, unzoomed_index_offset, field_count, defined_field_count, as_offset, total_summary_offset, uncompress_buf_size, extension_offset, level_list})
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Error: Please provide a filename!");
        std::process::exit(1);
    }
    match BigBed::from_file(&args[1]) {
        Ok(bb) => {
            println!("{:#?}", bb);
        }
        Err(msg) => {
            eprintln!("{}", msg);
        }
    }
}