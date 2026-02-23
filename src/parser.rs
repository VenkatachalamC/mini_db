use std::collections::HashMap;
use std::{collections::HashSet};
use pretty_table::table;

use crate::lexer::Lexer;

use crate::lexer::*;
use crate::processor::Processor;
use crate::table::{Column, ColumnType, DataType, ID_SIZE, INT_SIZE, STRING_SIZE};


pub struct Parser<'a>{
    lexer:Lexer<'a>,
    processor:&'a mut Processor
}

//Insert into users(age, name) values(1,kowshick)


impl<'a> Parser<'a> {
    pub fn new(lexer: Lexer<'a>, processor:&'a mut Processor)->Self{
        Parser{
            lexer,
            processor
        }
    }

    fn parse_select(&mut self) -> Result<(),String> {
        let mut column_names = HashSet::new();
        while let Token::Identifier(column_name) = self.lexer.consume() {
            column_names.insert(column_name);
            let next_token = self.lexer.consume();
            match next_token {
                Token::Keyword(KeyWords::COMMA) => continue,
                Token::Keyword(KeyWords::FROM) =>  break,
                _ => return Err("Error parsing Select Statement".to_string())
            }
        }
        let table_name = match self.lexer.consume() {
            Token::Identifier(table_name)=>table_name,
            _ =>return Err("Expected table name".to_string())
        };
        if column_names.len() == 0 {
            return Err(String::from("Columns not provided."));
        }
        println!("Table name: {}, Columns: {:?}",table_name, column_names);
        self.processor.handle_select_statement(&table_name, column_names)
    }

    fn parse_insert(&mut self) -> Result<(),String> {
        let into = self.lexer.consume();
        match into {
            Token::Keyword(KeyWords::INTO) => {},
            _ => return Err("Expected INTO keyword".to_string())
        }
        let table_name = match self.lexer.consume() {
            Token::Identifier(name) => name,
            _ => return Err("Expected table name".to_string())
        };
        let mut column_names = vec![];
        match self.lexer.consume() {
            Token::Keyword(KeyWords::LEFTPAREN)=>{},
            _ => return Err("Expected ( for Columns specifier".to_string())
        };
        while let Token::Identifier(column_name) = self.lexer.consume() {
            column_names.push(column_name);
            let next_token = self.lexer.consume();
            match next_token {
                Token::Keyword(KeyWords::COMMA) => continue,
                Token::Keyword(KeyWords::RIGHTPAREN) =>  break,
                _ => return Err("Error parsing Select Statement".to_string())
            }
        }
        match self.lexer.consume() {
            Token::Keyword(KeyWords::VALUES)=>{},
            _=> return Err("Expected Values for Inserting values".to_string())
        }
        match self.lexer.consume() {
            Token::Keyword(KeyWords::LEFTPAREN)=>{},
            _ => return Err("Expected ( for Columns specifier".to_string())
        };
        let mut column_values = vec![];
        while let Token::Identifier(column_value) = self.lexer.consume() {
            column_values.push(column_value);
            let next_token = self.lexer.consume();
            match next_token {
                Token::Keyword(KeyWords::COMMA) => continue,
                Token::Keyword(KeyWords::RIGHTPAREN) =>  break,
                _ => return Err("Error parsing Select Statement".to_string())
            }
        }
        if column_names.len() != column_values.len() {
            return Err("Colums and values doesn't match".to_string());
        }
        let colum_map:HashMap<String,String> = column_names.into_iter().zip(column_values.into_iter()).collect();
        return self.processor.handle_insert_statement(&table_name, colum_map);
    }

    fn parse_update(&mut self) -> Result<(),String> {
        // To be implemented
        Ok(())
    }
    fn parse_create(&mut self) -> Result<(),String> {
        let table_keyword = self.lexer.consume();
        println!("lexer {:?}",self.lexer);
        match table_keyword {
            Token::Keyword(KeyWords::TABLE)=>{},
            _=> return Err("Expected TABLE keyword".to_string())
        }
        let table_name = match self.lexer.consume() {
            Token::Identifier(name) => name,
            _ => return Err("Expected table name".to_string())
        };
        match self.lexer.consume() {
            Token::Keyword(KeyWords::LEFTPAREN)=>{},
            _ => return Err("Expected ( for Columns specifier".to_string())
        };
        let mut columns_meta:Vec<(String,DataType)> = vec![];
        loop {
            let column_name = match self.lexer.consume() {
                Token::Identifier(name) => name,
                _ => return Err("Expected column name".to_string())
            };
            let data_type = match self.lexer.consume() {
                Token::Keyword(KeyWords::INT)=>DataType::INT,
                Token::Keyword(KeyWords::STRING)=>DataType::STRING,
                _ => return Err("Expected data type".to_string())
            };
            columns_meta.push((column_name, data_type));
            let next_token = self.lexer.consume();
            match next_token {
                Token::Keyword(KeyWords::COMMA) => continue,
                Token::Keyword(KeyWords::RIGHTPAREN) =>  break,
                _ => return Err("Error parsing Columns".to_string())
            }
        }
        let mut columns = vec![];
        for (column_name, data_type) in columns_meta.iter(){
            let col = Column{
                name: column_name.clone(),
                data_type: data_type.clone(),
                size: match data_type {
                    DataType::INT=>INT_SIZE,
                    DataType::STRING=>STRING_SIZE,
                    DataType::UUID=>ID_SIZE
                },
                col_type: ColumnType::FIELD
            };
            columns.push(col);
        }
        self.processor.create_table(table_name, columns)?;
        Ok(())
    }

    fn parse_command(&mut self) -> Result<(),String> {
        let token = self.lexer.consume();
        if let Token::Keyword(keyword) = token {
            return match keyword {
                KeyWords::INSERT =>self.parse_insert(),
                KeyWords::SELECT =>self.parse_select(),
                KeyWords::UPDATE =>self.parse_update(),
                KeyWords::CREATE =>self.parse_create(),
                _=>return Err("Unsupported command".to_string()),
            };
        }
        Err("Unexpected token".to_string())
    }

    pub fn parse(&mut self) -> Result<(),String> {
        self.lexer.tokenize();
        self.parse_command()
    }
}


#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;

    use super::*;
    
    #[test]
    fn test_insert_missing_into() {
        let sql = "INSERT users (a) VALUES (b)";
        let mut lexer = Lexer::new(sql);
        lexer.tokenize();
        let mut dummy: MaybeUninit<Processor> = MaybeUninit::uninit();
        let processor = unsafe { &mut *dummy.as_mut_ptr() };
        let mut parser = Parser::new(lexer, processor);

        let res = parser.parse_insert();
        assert_eq!(res, Err("Expected INTO keyword".to_string()));
    }

    #[test]
    fn test_insert_missing_table_name() {
        let sql = "INSERT INTO (a) VALUES (b)";
        let mut lexer = Lexer::new(sql);
        lexer.tokenize();
        let mut dummy: MaybeUninit<Processor> = MaybeUninit::uninit();
        let processor = unsafe { &mut *dummy.as_mut_ptr() };
        let mut parser = Parser::new(lexer, processor);

        let res = parser.parse_insert();
        assert_eq!(res, Err("Expected table name".to_string()));
    }

    #[test]
    fn test_insert_missing_columns_paren() {
        let sql = "INSERT INTO users a VALUES (b)";
        let mut lexer = Lexer::new(sql);
        lexer.tokenize();
        let mut dummy: MaybeUninit<Processor> = MaybeUninit::uninit();
        let processor = unsafe { &mut *dummy.as_mut_ptr() };
        let mut parser = Parser::new(lexer, processor);

        let res = parser.parse_insert();
        assert_eq!(res, Err("Expected ( for Columns specifier".to_string()));
    }

    #[test]
    fn test_insert_invalid_columns_separator() {
        let sql = "INSERT INTO users (a b) VALUES (c d)";
        let mut lexer = Lexer::new(sql);
        lexer.tokenize();
        let mut dummy: MaybeUninit<Processor> = MaybeUninit::uninit();
        let processor = unsafe { &mut *dummy.as_mut_ptr() };
        let mut parser = Parser::new(lexer, processor);

        let res = parser.parse_insert();
        assert_eq!(res, Err("Error parsing Select Statement".to_string()));
    }

    #[test]
    fn test_insert_missing_values_keyword() {
        let sql = "INSERT INTO users (a,b) VALUE (c,d)";
        let mut lexer = Lexer::new(sql);
        lexer.tokenize();
        let mut dummy: MaybeUninit<Processor> = MaybeUninit::uninit();
        let processor = unsafe { &mut *dummy.as_mut_ptr() };
        let mut parser = Parser::new(lexer, processor);

        let res = parser.parse_insert();
        assert_eq!(res, Err("Expected Values for Inserting values".to_string()));
    }

    #[test]
    fn test_insert_missing_values_paren() {
        let sql = "INSERT INTO users (a,b) VALUES c,d";
        let mut lexer = Lexer::new(sql);
        lexer.tokenize();
        let mut dummy: MaybeUninit<Processor> = MaybeUninit::uninit();
        let processor = unsafe { &mut *dummy.as_mut_ptr() };
        let mut parser = Parser::new(lexer, processor);

        let res = parser.parse_insert();
        assert_eq!(res, Err("Expected ( for Columns specifier".to_string()));
    }

    #[test]
    fn test_insert_mismatched_counts() {
        let sql = "INSERT INTO users (a,b) VALUES (c)";
        let mut lexer = Lexer::new(sql);
        lexer.tokenize();
        let mut dummy: MaybeUninit<Processor> = MaybeUninit::uninit();
        let processor = unsafe { &mut *dummy.as_mut_ptr() };
        let mut parser = Parser::new(lexer, processor);

        let res = parser.parse_insert();
        assert_eq!(res, Err("Colums and values doesn't match".to_string()));
    }
}


