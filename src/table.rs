use pretty_table::prelude::*;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    error::Error,
    fs::{self, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
    rc::{Rc, Weak},
};
use uuid::Uuid;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub const ID_SIZE: usize = std::mem::size_of::<Uuid>();
pub const INT_SIZE: usize = std::mem::size_of::<u32>();
pub const STRING_SIZE: usize = 200;

const INT_DEFAULT: &str = "0";
const STRING_DEFAULT: &str = "";

const META_SIZE: usize = 4096;

//pages should be allocated from eof.
//all pages must have pointer to next page.

/*32+2+1+8+(32+1)*Number of columns */
const TABLE_NAME_SIZE: usize = 32;
const TOTAL_COLUMNS_INFO_SIZE: usize = 1;
const TOTAL_ROWS_SIZE:usize = 2;
const TABLE_DATA_LOCATION_SIZE: usize = 8;
const COLUMN_NAME_SIZE: usize = 32;
//0 for ID, 1 for INT, 2 for STRING
const COLUMN_TYPE_META: usize = 1;

#[derive(Debug, Clone)]
pub enum DataType {
    INT,
    STRING,
    UUID,
}

#[derive(Debug, Clone)]
pub enum ColumnType {
    ID,
    FIELD,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub size: usize,
    pub col_type: ColumnType,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: String, size: usize, col_type: ColumnType, data_type: DataType) -> Self {
        Column {
            name,
            size,
            col_type,
            data_type,
        }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Table {
    table_name: String,
    pages: Vec<Option<Page>>,
    columns: Vec<Column>,
    total_rows: usize,
    start_offset: usize,
    data_base: Weak<DataBase>,
}

//TODO fetch FIRST PAGE ON TABLE CREATION.
impl Table {
    pub fn new(
        name: String,
        columns: Vec<Column>,
        data_base: Weak<DataBase>,
        total_rows: usize,
        start_offset: usize,
    ) -> Self {

        let mut hasIdColumn = false;
        for column in columns.iter(){
            match column.col_type {
                ColumnType::ID => hasIdColumn = true,
                _=>{}
            }
        }

        let mut columns = columns;
        if !hasIdColumn {
            columns.insert(0, Column::new("id".to_string(), ID_SIZE, ColumnType::ID, DataType::UUID));
        }
        let table = Table {
            table_name: name,
            pages: vec![],
            columns,
            total_rows: 0,
            start_offset,
            data_base,
        };

        // let file_size: usize = table
        //     .data_base
        //     .upgrade()
        //     .unwrap()
        //     .file_handle
        //     .borrow()
        //     .metadata()
        //     .unwrap()
        //     .len() as usize
        //     - META_SIZE;
        // let row_size = table.row_size();
        // table.total_rows = file_size / row_size;
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
        let data_base = self.data_base.upgrade().unwrap();
        let mut file_handle = data_base.file_handle.borrow_mut();
        file_handle
            .seek(io::SeekFrom::Start(
                ((page_number * PAGE_SIZE) + self.start_offset) as u64,
            ))
            .unwrap();
        let page = self.pages[page_number].as_mut().unwrap();
        let bytes_read = file_handle
            .read(&mut page.data)
            .expect("Failed to read page from disk");
        page.current_row = bytes_read / page.row_size;
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
        let data_base = self.data_base.upgrade().unwrap();
        let file_handle = &mut data_base.file_handle.borrow_mut();
        let p = match &self.pages[page_index] {
            Some(page) => page,
            //To be handled properly
            None => return,
        };
        file_handle
            .seek(io::SeekFrom::Start(
                ((page_index * PAGE_SIZE) + self.start_offset) as u64,
            ))
            .unwrap();
        file_handle
            .write_all(&p.data[0..(p.current_row * p.row_size)])
            .unwrap();
        file_handle.flush().unwrap();
    }

    pub fn print_table(&mut self, columns: HashSet<String>) {
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
        let mut table_meta: Vec<String> = vec![];
        for (col_name, _, _) in &columns_meta {
            if columns.contains(&("*".to_string())) || columns.contains(&col_name[..]) {
                table_meta.push(col_name.clone());
            }
        }
        row_values.push(table_meta);
        println!("total rows {}", self.total_rows);
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

                let trim_len = field
                    .iter()
                    .rposition(|&b| b != 0)
                    .map(|i| i + 1)
                    .unwrap_or(0);
                let bytes = &field[..trim_len];
                if !columns.contains(&("*".to_string())) && !columns.contains(name) {
                    continue;
                }
                match col_type {
                    ColumnType::ID => {
                        if let Ok(value) = Uuid::from_slice(bytes) {
                            field_values.push(value.hyphenated().to_string());
                        } else {
                            field_values.push(String::from(""));
                        }
                    }
                    ColumnType::FIELD => {
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
            .map(|col: &Column| vec![0; col.size])
            .collect::<Vec<Vec<u8>>>();
        let id: Uuid = Uuid::now_v7();
        let mut row_data = vec![];
        row_data.extend_from_slice(id.as_bytes());
        for (i, arg) in args.iter().enumerate() {
            let bytes = arg.as_bytes();
            let buffer: &mut Vec<u8> = &mut field_buffers[i+1];
            let len = bytes.len().min(buffer.len());
            buffer[0..len].copy_from_slice(&bytes[0..len]);
            row_data.extend_from_slice(buffer);
        }
        row_data
    }

    pub fn match_columns(&self, column_map: HashMap<String, String>) -> Vec<String> {
        let mut row_values: Vec<String> = vec![];
        for column in &self.columns {
            let buffer = match column.col_type {
                ColumnType::FIELD => Some(
                    column_map
                        .get(&column.name)
                        .unwrap_or(&(|| match column.data_type {
                            DataType::INT => INT_DEFAULT.to_string(),
                            DataType::STRING => STRING_DEFAULT.to_string(),
                            _ => STRING_DEFAULT.to_string(),
                        })())
                        .clone(),
                ),
                ColumnType::ID => None,
            };
            if let Some(value) = buffer {
                row_values.push(value);
            }
        }
        row_values
    }

    fn get_table_meta(&mut self) -> Vec<u8> {
        let mut table_info_size = 0;
        table_info_size += 43;
        table_info_size += 33 * self.columns.len();

        let mut buff: Vec<u8> = Vec::new();
        buff.resize(table_info_size, 0u8);

        let mut table_name_buff = [0u8; 32];
        table_name_buff[0..self.table_name.len()].copy_from_slice(self.table_name.as_bytes());
        buff[0..32].copy_from_slice(&table_name_buff);

        buff[32] = self.columns.len() as u8;
        buff[33..35].copy_from_slice(&(self.total_rows as u16).to_le_bytes());
        buff[35..43].copy_from_slice(&(self.start_offset as u64).to_le_bytes());
        let mut offset = 43;

        for column in self.columns.iter() {
            let mut column_name_buff = [0u8; 32];
            column_name_buff[0..column.name.len()].copy_from_slice(column.name.clone().as_bytes());
            buff[offset..offset + 32].copy_from_slice(&column_name_buff);

            buff[offset + 32] = match column.data_type.clone() {
                DataType::UUID => 0,
                DataType::STRING => 1,
                DataType::INT => 2,
            };
            offset += 33;
        }
        buff
    }
}

pub struct DataBase {
    pub tables: RefCell<HashMap<String, Rc<RefCell<Table>>>>,
    pub num_tables: RefCell<u8>,
    file_handle: RefCell<fs::File>,
}

impl DataBase {
    pub fn new(file_name: String) -> Rc<Self> {
        let tables: RefCell<HashMap<String, Rc<RefCell<Table>>>> = RefCell::new(HashMap::new());
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(Path::new(&file_name))
            .expect("Failed to open file");
        let mut table_meta = [0; META_SIZE];
        let _ = file.read(&mut table_meta).unwrap();

        let num_tables = table_meta[0];

        let mut i = 0;

        let mut offset: usize = 1;
        let database = Rc::new(DataBase {
            tables,
            num_tables: RefCell::new(num_tables),
            file_handle: RefCell::new(file),
        });
        while i < num_tables {
            let table_name: String =
                String::from_utf8_lossy(&table_meta[offset..offset + TABLE_NAME_SIZE]).replace('\0', "").trim().to_string();
            offset += TABLE_NAME_SIZE;
            let mut num_columns: u8 = table_meta[offset];
            offset += TOTAL_COLUMNS_INFO_SIZE;
            let total_rows: u16 = u16::from_le_bytes(
                table_meta[offset..offset + TOTAL_ROWS_SIZE]
                    .try_into()
                    .unwrap(),
            );
            offset += TOTAL_ROWS_SIZE;
            let start_index = u64::from_le_bytes(
                table_meta[offset..offset + TABLE_DATA_LOCATION_SIZE]
                    .try_into()
                    .unwrap(),
            );
            offset += TABLE_DATA_LOCATION_SIZE;
            let mut colums = vec![];

            while num_columns > 0 {
                let column_name = &table_meta[offset..offset + COLUMN_NAME_SIZE];
                let column_type: u8 = table_meta[offset + COLUMN_NAME_SIZE];
                let column: Column = match column_type {
                    0 => Column {
                        name: String::from_utf8_lossy(column_name).replace('\0', "").trim().to_string(),
                        size: ID_SIZE,
                        col_type: ColumnType::ID,
                        data_type: DataType::UUID,
                    },
                    1 => Column {
                        name: String::from_utf8_lossy(column_name).replace('\0', "").trim().to_string(),
                        size: STRING_SIZE,
                        col_type: ColumnType::FIELD,
                        data_type: DataType::STRING,
                    },
                    2 => Column {
                        name: String::from_utf8_lossy(column_name).replace('\0', "").trim().to_string(),
                        size: INT_SIZE,
                        col_type: ColumnType::FIELD,
                        data_type: DataType::INT,
                    },
                    _ => panic!("Invalid column type"),
                };
                colums.push(column);
                offset += COLUMN_NAME_SIZE + COLUMN_TYPE_META;
                num_columns -= 1;
            }
            database.add_table(table_name, colums, total_rows as usize,Some(start_index as usize), false);
            i+=1;
        }
        database
    }

    pub fn add_table(
        self: &Rc<Self>,
        table_name: String,
        columns: Vec<Column>,
        total_rows:usize,
        start_offset: Option<usize>,
        flush:bool
    ) {
        let weak_db: Weak<DataBase> = Rc::downgrade(self);
        let table_start_offset = match start_offset {
            Some(offset) => offset,
            None => {
                let tables = self.tables.borrow();
                let mut occupied_page = 0;
                for table in tables.values() {
                    let table_borrow = Rc::clone(table);
                    let table_pages = ((table_borrow.borrow().total_rows / PAGE_SIZE) as f64).ceil() as usize;
                    occupied_page += table_pages;
                }
                occupied_page + META_SIZE
            }
        };
        let table: Table = Table::new(
                table_name.clone(),
                columns,
                weak_db,
                total_rows,
                table_start_offset,
            );
        self.create_table(table, flush ).unwrap();
    }

    fn create_table(self: &Rc<Self>, table: Table, flush:bool) -> Result<(), String> {
        self.tables
            .borrow_mut()
            .insert(table.table_name.clone(), Rc::new(RefCell::new(table)));
        *self.num_tables.borrow_mut() += 1;
        if flush {
            self.flush();
        }
        Ok(())
    }

    fn flush(self: &Rc<Self>) {
        let mut buff = [0; META_SIZE];
        buff[0] = *self.num_tables.borrow();
        let mut offset = 1;
        println!("Number of tables: {}", *self.num_tables.borrow());
        println!("Tables: {:?}", self.tables.borrow().keys());
        for table in self.tables.borrow_mut().values_mut() {
            let table_borrow = Rc::clone(table);
            let table_meta: Vec<u8> = table_borrow.borrow_mut().get_table_meta();
            buff[offset..offset + table_meta.len()].copy_from_slice(&table_meta);
            offset += table_meta.len();
        }
        self.file_handle
            .borrow_mut()
            .seek(io::SeekFrom::Start(0))
            .unwrap();
        self.file_handle.borrow_mut().write_all(&buff).unwrap();
        self.file_handle.borrow_mut().flush().unwrap();
    }
}
