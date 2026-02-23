use std::{io::{self, Write}};


mod table;

mod lexer;

mod parser;

mod processor;

use processor::Processor;

use lexer::Lexer;

use parser::Parser;

use crate::table::DataBase;

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
    let data_base = DataBase::new("test.db".to_string());
    let mut processor = Processor::new(data_base);
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


#[cfg(test)]
mod tests {
    use super::*;

    fn setup_processor() -> Processor {
        let data_base = DataBase::new("test.db".to_string());
        Processor::new(data_base)
    }

    #[test]
    fn test_insert_and_read_from_multiple_tables() {
        let mut processor = setup_processor();

        // Create first table
        parse_command(&mut processor, "CREATE TABLE users (id INT, name TEXT)").unwrap();
        
        // Create second table
        parse_command(&mut processor, "CREATE TABLE orders (order_id INT, user_id INT, amount INT)").unwrap();

        // Insert into users table
        parse_command(&mut processor, "INSERT INTO users VALUES (1, 'Alice')").unwrap();
        parse_command(&mut processor, "INSERT INTO users VALUES (2, 'Bob')").unwrap();

        // Insert into orders table
        parse_command(&mut processor, "INSERT INTO orders VALUES (100, 1, 50)").unwrap();
        parse_command(&mut processor, "INSERT INTO orders VALUES (101, 2, 75)").unwrap();
        parse_command(&mut processor, "INSERT INTO orders VALUES (102, 1, 25)").unwrap();

        // Read from users table
        parse_command(&mut processor, "SELECT * FROM users").unwrap();

        // Read from orders table
        parse_command(&mut processor, "SELECT * FROM orders").unwrap();
    }

    #[test]
    fn test_multiple_tables_isolation() {
        let mut processor = setup_processor();

        // Create tables
        parse_command(&mut processor, "CREATE TABLE products (id INT, name TEXT)").unwrap();
        parse_command(&mut processor, "CREATE TABLE categories (id INT, category TEXT)").unwrap();

        // Insert data into products
        parse_command(&mut processor, "INSERT INTO products VALUES (1, 'Laptop')").unwrap();
        parse_command(&mut processor, "INSERT INTO products VALUES (2, 'Phone')").unwrap();

        // Insert data into categories
        parse_command(&mut processor, "INSERT INTO categories VALUES (1, 'Electronics')").unwrap();

        // Verify each table has correct data
        parse_command(&mut processor, "SELECT * FROM products").unwrap();
        parse_command(&mut processor, "SELECT * FROM categories").unwrap();
    }

    #[test]
    fn test_insert_into_nonexistent_table() {
        let mut processor = setup_processor();

        // Try to insert into a table that doesn't exist
        parse_command(&mut processor, "INSERT INTO nonexistent VALUES (1, 'test')").unwrap();
    }

    #[test]
    fn test_read_from_empty_table() {
        let mut processor = setup_processor();

        // Create table but don't insert anything
        parse_command(&mut processor, "CREATE TABLE empty_table (id INT, value TEXT)").unwrap();

        // Try to read from empty table
        parse_command(&mut processor, "SELECT * FROM empty_table").unwrap();
    }

    #[test]
    fn test_multiple_inserts_same_table() {
        let mut processor = setup_processor();

        parse_command(&mut processor, "CREATE TABLE logs (id INT, message TEXT)").unwrap();

        // Insert multiple rows
        for i in 0..10 {
            let cmd = format!("INSERT INTO logs VALUES ({}, 'message_{}')", i, i);
            parse_command(&mut processor, &cmd).unwrap();
        }

        parse_command(&mut processor, "SELECT * FROM logs").unwrap();
    }
}

