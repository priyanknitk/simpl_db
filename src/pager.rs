use std::io::{Read, Seek, Write};
use crate::constants::*;

pub struct Pager {
    file: std::fs::File,
    pub num_pages: usize,
    pub pages: Vec<Option<[u8; PAGE_SIZE]>>,
}

impl Pager {
    pub fn open(file_name: &str) -> Self {
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
        {
            Ok(file) => file,
            Err(err) => {
                eprintln!("Error opening file: {}", err);
                panic!("Error opening file.");
            }
        };
        let mut pages = Vec::new();
        let file_length = file.metadata().unwrap().len() as usize;
        let num_pages = file_length / PAGE_SIZE;
        if file_length % PAGE_SIZE != 0 {
            eprintln!("Db file is not a whole number of pages. Corrupt file.");
            panic!("Db file is not a whole number of pages.");
        }
        for _ in 0..TABLE_MAX_PAGES {
            pages.push(None);
        }
        Pager {
            file,
            num_pages,
            pages,
        }
    }

    fn file_length(&self) -> usize {
        self.file.metadata().unwrap().len() as usize
    }

    pub fn pager_flush(&mut self, page_num: usize) {
        let page = self.pages[page_num].as_ref().unwrap_or_else(|| {
            eprintln!("Tried to flush null page.");
            panic!("Tried to flush null page.");
        });
        self.file
            .seek(std::io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .unwrap_or_else(|e| {
                eprintln!("Error seeking: {}", e);
                panic!("Error seeking.");
            });
        self.file
            .write_all(&page[0..PAGE_SIZE])
            .unwrap_or_else(|e| {
                eprintln!("Error writing file: {}", e);
                panic!("Error writing file.");
            });
    }

    pub fn get_page(&mut self, page_num: usize) -> &mut [u8] {
        if page_num > TABLE_MAX_PAGES {
            eprintln!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
            panic!("Tried to fetch page number out of bounds.");
        }
        if self.pages[page_num].is_none() {
            let mut page = [0; PAGE_SIZE];
            let file_length = self.file_length();
            let mut num_pages = file_length / PAGE_SIZE;
            // if the file length is not a multiple of the page size, we have a partial page
            if file_length % PAGE_SIZE > 0 {
                num_pages += 1;
            }
            if page_num <= num_pages {
                self.file
                    .seek(std::io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .unwrap_or_else(|e| {
                        eprintln!("Error seeking: {}", e);
                        panic!("Error seeking.");
                    });
                self.file.read(&mut page).unwrap_or_else(|e| {
                    eprintln!("Error reading file: {}", e);
                    panic!("Error reading file.");
                });
            }
            self.pages[page_num] = Some(page);
            if page_num >= self.num_pages {
                self.num_pages = page_num + 1;
            }
        }
        self.pages[page_num].as_mut().unwrap()
    }

    pub fn get_unused_page_num(&self) -> usize {
        self.num_pages
    }
}
