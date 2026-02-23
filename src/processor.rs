use std::{collections::{HashMap, HashSet}};

use crate::table::Table;

pub struct Processor {
    tables: HashMap<String, Table>
}

impl Processor {
    pub fn new(tables:HashMap<String, Table>)->Self {
        Processor { tables }
    }
    pub fn handle_select_statement(&mut self,table_name: &str, colums: HashSet<String>) -> Result<(),String>{
        let table = self.tables.get_mut(table_name);
        match table {
            Some(table) => table.print_table(colums),
            None=> return Err(String::from("Table not found"))
        }
        
        Ok(())
    }

    pub fn handle_insert_statement(&mut self, table_name:&str, column_map:HashMap<String,String>)->Result<(),String>{
        let table = self.tables.get_mut(table_name);
        match table {
            Some(table)=> {
                let row_vector = table.match_columns(column_map);
                let row = &table.construct_row(row_vector);
                return match table.insert_rows(vec![row]){
                    Ok(_)=>Ok(()),
                    Err(e)=>Err(format!("Error inserting row: {}",e))
                }
            } ,
           None=> return Err(String::from("Table not found"))
        }
    }
}