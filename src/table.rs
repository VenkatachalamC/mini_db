use std::{collections::{HashMap, HashSet}, error::Error, fs::{self, OpenOptions}, io::{self, Read, Seek, Write}, path::Path};
use uuid::{Uuid};
use pretty_table::prelude::*;



pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub const ID_SIZE: usize = std::mem::size_of::<Uuid>();
pub const INT_SIZE: usize = std::mem::size_of::<u32>();
pub const STRING_SIZE: usize = 200;

const INT_DEFAULT: &str = "0";
const STRING_DEFAULT: &str = "";

#[derive(Debug,Clone)]
pub enum DataType{
    INT,
    STRING,
    UUID
}

#[derive(Debug, Clone)]
pub enum ColumnType{
    ID,
    FIELD
}

#[derive(Debug,Clone)]
pub struct Column {
    pub name: String,
    pub size: usize,
    pub col_type: ColumnType,
    pub data_type: DataType
}

impl Column {
    pub fn new(name: String, size: usize, col_type:ColumnType,data_type:DataType ) -> Self {
        Column {
            name,
            size,
            col_type,
            data_type
        }
    }
}

pub struct Page {
    data: [u8; PAGE_SIZE],
    row_size: usize,
    current_row: usize,
}

//To be done:
//Refactor code.
//Add parsing and more functionality.
//Implement B+ tree for indexing.
//HANDLE ERRORS PROPERLY


impl Page {
    pub fn new(row_size: usize) -> Self {
        Page {
            data: [0; PAGE_SIZE],
            row_size,
            current_row: 0,
        }
    }

    pub fn read_row(&self, row_number: usize) -> &[u8] {
        let start = row_number * self.row_size;
        &self.data[start..start + self.row_size]
    }

    pub fn write_row(&mut self, data: &[u8]) -> Result<(), Box<dyn Error>> {
        let start = self.current_row * self.row_size;
        self.data[start..start + self.row_size].copy_from_slice(data);
        self.current_row += 1;
        Ok(())
    }

    // pub fn update_row(&mut self, row_number: usize, data: &[u8]) -> Result<(), Box<dyn Error>> {
    //     let start = row_number * self.row_size;
    //     self.data[start..start + self.row_size].copy_from_slice(data);
    //     Ok(())
    // }
}
pub struct Table {
    table_name: String,
    pages: Vec<Option<Page>>,
    columns: Vec<Column>,
    total_rows: usize,
    file_handle: fs::File,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>,file_name: String) -> Self {
        let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(Path::new(&file_name)).expect("Failed to open file");
        let mut table = Table {
            table_name: name,
            pages: vec![],
            columns: [vec![Column::new("id".to_string(),ID_SIZE, ColumnType::ID, DataType::UUID)],columns].concat(),
            total_rows: 0,
            file_handle: file,
        };

        let file_size = table.file_handle.metadata().unwrap().len() as usize;
        let row_size = table.row_size();
        table.total_rows = file_size / row_size;
        table
    }

    fn row_size(&self) -> usize {
        let mut size: usize = 0;
        for column in self.columns.iter() {
            size += column.size
        }
        size
    }

    fn get_page(&mut self, page_number: usize) -> &mut Page {
        if self.pages.len() >= TABLE_MAX_PAGES {
            panic!("Table full.");
        }

        if self.pages.len() <= page_number {
            self.pages.resize_with(page_number + 1, || None);
        }
        if self.pages[page_number].is_none() {
            let row_size = self.row_size();
            self.pages[page_number] = Some(Page::new(row_size));
            if self.total_rows > page_number * (PAGE_SIZE / self.row_size()) {
                self.fill_page_from_disk(page_number);
            }
        }
        self.pages[page_number].as_mut().unwrap()
    }

    fn fill_page_from_disk(&mut self, page_number: usize) {
        let file_handle = &mut self.file_handle;
        file_handle
            .seek(io::SeekFrom::Start((page_number * PAGE_SIZE) as u64))
            .unwrap();
        let page = self.pages[page_number].as_mut().unwrap();
        let bytes_read =file_handle
            .read(&mut page.data)
            .expect("Failed to read page from disk");
        page.current_row = bytes_read/ page.row_size;
    }

    pub fn insert_rows(&mut self, values: Vec<&[u8]>) -> Result<(), Box<dyn Error>> {
        for value in values {
            let row_size = self.row_size();
            let rows_per_page = PAGE_SIZE / row_size;
            let current_page = self.total_rows / rows_per_page;
            let page = self.get_page(current_page);
            page.write_row(value)?;
            self.flush_page_to_disk(current_page);
            self.total_rows += 1;

        }
        Ok(())
    }

    fn flush_page_to_disk(&mut self, page_index: usize) {
        let file_handle = &mut self.file_handle;
        let p = match &self.pages[page_index] {
            Some(page) => page,
            //To be handled properly
            None => return ,
        };
        file_handle.seek(io::SeekFrom::Start((page_index * PAGE_SIZE) as u64)).unwrap();
        file_handle.write_all(&p.data[0..(p.current_row* p.row_size)]).unwrap();
        file_handle.flush().unwrap();
    }

    pub fn print_table(&mut self, columns:HashSet<String>) {
        let row_size = self.row_size();
        if row_size == 0 || PAGE_SIZE < row_size {
            return;
        }
        let rows_per_page = PAGE_SIZE / row_size;
        let columns_meta: Vec<(String, usize, ColumnType)> = self
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.size, c.col_type.clone()))
            .collect();
        println!("Table: {} [{} rows]", self.table_name, self.total_rows);
        let mut row_values: Vec<Vec<String>> = vec![];
        let mut table_meta:Vec<String>  = vec![];
        for (col_name, _, _) in &columns_meta {
            if columns.contains(&("*".to_string())) || columns.contains(&col_name[..]) {
                table_meta.push(col_name.clone());
            }
        }
        row_values.push(table_meta);
        println!("total rows {}",self.total_rows);
        for row_number in 0..self.total_rows {
            let page_number = row_number / rows_per_page;
            let row_index = row_number % rows_per_page;
            let page = self.get_page(page_number);
            let row = page.read_row(row_index);

            let mut offset = 0;
            let mut field_values: Vec<String> = vec![];
            for (name, size, col_type) in &columns_meta {
                let end = offset + *size;
                if end > row.len() {
                    break;
                }
                let field = &row[offset..end];
                offset = end;

                let trim_len = field.iter().rposition(|&b| b != 0).map(|i| i + 1).unwrap_or(0);
                let bytes = &field[..trim_len];
                if !columns.contains(&("*".to_string())) && !columns.contains(name) {
                    continue;
                }
                match col_type {
                    ColumnType::ID =>{
                        if let Ok(value) = Uuid::from_slice(bytes){
                        field_values.push(value.hyphenated().to_string());
                    } else {
                        field_values.push(String::from(""));
                    }
                    },
                    ColumnType::FIELD=> {
                        let value: std::borrow::Cow<'_, str> = String::from_utf8_lossy(bytes);
                        field_values.push(value.to_string());
                    }
                }
            }
            row_values.push(field_values.clone());
        }
        print_table!(row_values.clone());
    }

    // pub fn update_row(&mut self, row_number: usize, data: &[u8]) -> Result<(), Box<dyn Error>> {
    //     let row_size = self.row_size();
    //     let rows_per_page = PAGE_SIZE / row_size;
    //     let page_number = row_number / rows_per_page;
    //     let page = self.get_page(page_number);
    //     let row_index = row_number % rows_per_page;
    //     page.update_row(row_index, data)?;
    //     self.flush_page_to_disk(page_number);
    //     Ok(())
    // }
    pub fn construct_row(&mut self, args: Vec<String>) -> Vec<u8> {
        // TO be refactored
        let mut field_buffers = self
            .columns
            .iter()
            .map(|col: &Column|vec![0; col.size])
            .collect::<Vec<Vec<u8>>>();
        let id: Uuid = Uuid::now_v7();
        let mut row_data = vec![];
        row_data.extend_from_slice(id.as_bytes());
        for (i, arg) in args.iter().enumerate() {
            let bytes = arg.as_bytes();
            let buffer = &mut field_buffers[i+1];
            let len = bytes.len().min(buffer.len());
            buffer[0..len].copy_from_slice(&bytes[0..len]);
            row_data.extend_from_slice(buffer);
        }
        row_data
    }

    pub fn match_columns(&self, column_map: HashMap<String, String>) -> Vec<String>{
        let mut row_values:Vec<String> = vec![];
        for column in &self.columns{
                let buffer = match column.col_type {
                    ColumnType::FIELD => Some(column_map.get(&column.name).unwrap_or(&(||
                        match column.data_type {
                            DataType::INT =>INT_DEFAULT.to_string(),
                            DataType::STRING=>STRING_DEFAULT.to_string(),
                            _=>STRING_DEFAULT.to_string()
                        }
                    )()).clone()),
                    ColumnType::ID => None
            };
            if let Some(value) = buffer{
                row_values.push(value);
            }
        }
        row_values
    }

}
