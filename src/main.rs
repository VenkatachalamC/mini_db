use std::{collections::{HashMap}, io::{self, Write}};


mod table;
use table::{Column, Table};

mod lexer;

mod parser;

mod processor;

use processor::Processor;

use lexer::Lexer;

use parser::Parser;

//todo
//B+tree implementation for indexing and searching
//parser implementation for Create table and update table.

fn parse_command(processor: &mut Processor, input: &str) -> Result<(), io::Error> {
    let lexer = Lexer::new(input);
    let mut parser = Parser::new(lexer,processor);
    match parser.parse(){
        Err(error)=>println!("{}",error),
        _=>{}
    }
    Ok(())
}
fn main() -> Result<(), io::Error> {
    let columns: Vec<Column> = vec![
        Column {
            name: "age".to_string(),
            size: table::INT_SIZE,
            col_type: table::ColumnType::FIELD,
            data_type: table::DataType::INT
        },
        Column {
            name: "name".to_string(),
            size: table::STRING_SIZE,
            col_type: table::ColumnType::FIELD,
            data_type: table::DataType::STRING
        },
    ];
    let table: Table = Table::new("users".to_string(), columns,String::from("test_db_file.db"));
    let mut table_map = HashMap::new();
    table_map.insert("users".to_string(), table);
    let mut processor = Processor::new(table_map);
    let mut input = String::new();
    loop {
        print!("test_db>");
        io::stdout().flush()?;
        input.clear();
        let n = io::stdin().read_line(&mut input)?;
        if n == 0 {
            break;
        }
        match input.trim() {
            ".exit" => break,
            command => parse_command(&mut processor, command)?,
        }
    }

    return Ok(());
}
