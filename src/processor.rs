use std::{collections::{HashMap, HashSet}, rc::Rc};

use crate::table::{Column, DataBase};

pub struct Processor {
    database: Rc<DataBase>,
}

impl Processor {
    pub fn new(database:Rc<DataBase>)->Self {
        Processor { database }
    }
    pub fn handle_select_statement(&mut self,table_name: &str, colums: HashSet<String>) -> Result<(),String>{
        let mut tables = self.database.tables.borrow_mut();
        println!("{:?}",tables);
        let table = tables.get_mut(table_name);
        println!("Selected Table: {}",table_name);
        match table {
            Some(table) => table.borrow_mut().print_table(colums),
            None=> return Err(String::from("Table not found"))
        }
        
        Ok(())
    }

    pub fn handle_insert_statement(&mut self, table_name:&str, column_map:HashMap<String,String>)->Result<(),String>{
        let mut tables = self.database.tables.borrow_mut();
        let table = tables.get_mut(table_name);
        match table {
            Some(table)=> {
                let row_vector = table.borrow_mut().match_columns(column_map);
                let row = &table.borrow_mut().construct_row(row_vector);
                return match table.borrow_mut().insert_rows(vec![row]){
                    Ok(_)=>Ok(()),
                    Err(e)=>Err(format!("Error inserting row: {}",e))
                }
            } ,
           None=> return Err(String::from("Table not found"))
        }
    }

    pub fn create_table(&mut self, table_name:String, columns:Vec<Column>)->Result<(),String>{
        self.database.add_table(table_name, columns, 0, None, true);
        Ok(())
    }
}