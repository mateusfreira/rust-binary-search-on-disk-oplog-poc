//let search: u64 = 1618686833249;// wrost case not found bigger
//let search: u64 = 1; // wrost case not found smaller
// Best case 1618686829920
use std::fmt;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::SeekFrom;
use std::str;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{thread, time};

const KEY_SIZE: usize = 8;
const TIME_SIZE: usize = 8;
const RECORD_SIZE: usize = TIME_SIZE + KEY_SIZE + 1;
const WRITE: bool = true;
const READ: bool = true;

enum Opp {
    Update = 0,
    Delete = 1,
}


impl From<u8> for Opp {
    fn from(val: u8) -> Self {
        use self::Opp::*;
        match val {
            0 => Update,
            1 => Delete,
            _ => Update,
        }
    }
}

impl fmt::Display for Opp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Opp::Update => write!(f, "Opdate"),
            Opp::Delete => write!(f, "Delete"),
        }
    }
}

//  [] add key to the storeage
//
fn main() {
    let handle = thread::spawn(|| {
        if !READ {
            return;
        }
        let search: u64 = 1618765137297; // wrost case found
        let mut f = OpenOptions::new().read(true).open("test.nun.log").unwrap();
        let size_as_u64 = RECORD_SIZE as u64;
        let total_size = f.metadata().unwrap().len();
        let mut max = total_size;
        let mut min = 0;
        let mut seek_point = (total_size / size_as_u64 / 2) * size_as_u64;
        let records = total_size / RECORD_SIZE as u64;

        println!("Total size {}, Recoreds {}", total_size, records);
        let mut buffer = [0; TIME_SIZE];
        let mut key_buffer = [0; KEY_SIZE];
        let mut oop_buffer = [0; 1];
        f.seek(SeekFrom::Start(seek_point)).unwrap();
        let now = time::Instant::now();
        while let Ok(i) = f.read(&mut buffer) {
            //Read key from disk
            let possible_records = (max - min) / size_as_u64;
            let n = u64::from_le_bytes(buffer);

            println!("number {} to compare", n);
            if n == search {
                let e = now.elapsed();
                f.read(&mut key_buffer).unwrap();
                let key:u64 = u64::from_le_bytes(key_buffer); 
                //String = str::from_utf8(&key_buffer).unwrap().trim_end().to_string();
                f.read(&mut oop_buffer).unwrap();
                let oop = Opp::from(oop_buffer[0]);
                println!(
                    "found the number {} in {:?} key: {} oop: {}",
                    n, e, key, oop
                );
                break;
            }

            if n > search {
                max = seek_point;
                //println!( "seach smaller {} in {:?} {} {}", n, now.elapsed(), seek_point, possible_records );
                let n_recors: u64 = ((seek_point - min) / 2) / size_as_u64;
                seek_point = seek_point - (n_recors * size_as_u64);
            }

            if n < search {
                min = seek_point;
                let n_recors: u64 = ((max - seek_point) / 2) / size_as_u64;
                //println!("search bigger {} in {:?} {} {}",n,now.elapsed(), seek_point, possible_records );
                seek_point = (n_recors * size_as_u64) + seek_point;
            }

            if possible_records <= 1 {
                println!(" Did not found {:?} {}, {}", now.elapsed(), max, seek_point);
                break;
            }

            if i != TIME_SIZE as usize {
                println!(" End of the file{:?} ms", now.elapsed());
                break;
            }

            f.seek(SeekFrom::Start(seek_point)).unwrap(); // Repoint disk seek
        }
    });

    if WRITE {
        let file = OpenOptions::new()
            .append(true)
            .open("test.nun.log")
            .unwrap();
        let mut stream = BufWriter::with_capacity(RECORD_SIZE, file);
        let now = time::Instant::now();
        for _ in 0..1000000 {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let in_ms = since_the_epoch.as_secs() * 1000
                + since_the_epoch.subsec_nanos() as u64 / 1_000_000;
            //let to_write:[&u8] = [in_ms.to_le_bytes(), key.as_bytes()].concat();
            //println!("write number  {}", in_ms);
            let key:u64 = 1;
            let opp = Opp::Delete;
            let opp_to_write = opp as u8;
            stream.write(&in_ms.to_le_bytes()).unwrap(); //8 bytes
            stream.write(&key.to_le_bytes()).unwrap(); // 8
            stream.write(&[opp_to_write]).unwrap(); // 1 byte

            if let Err(e) = stream.flush() {
                eprintln!("Couldn't write to file: {}", e);
            }
            //let ten_millis = time::Duration::from_millis(1);
            //thread::sleep(ten_millis);
        }
        println!("Time to write {:?}", now.elapsed());
    }

    if READ {
        handle.join().unwrap();
    }
}
