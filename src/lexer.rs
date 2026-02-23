#[derive(Debug, Clone)]
pub enum KeyWords {
    CREATE,
    SELECT,
    INSERT,
    UPDATE,
    COMMA,
    LEFTPAREN,
    RIGHTPAREN,
    STRING,
    INT,
    FROM,
    INTO,
    VALUES
}

#[derive(Debug, Clone)]
pub enum Token {
    Keyword(KeyWords),
    Identifier(String),
    EOL
}

#[derive(Debug)]
pub struct Lexer<'a> {
    input:&'a str,
    tokens:Vec<Token>,
}

impl<'a> Lexer <'a>{
    pub fn new(input:&'a str) -> Self {
        Lexer {
            input,
            tokens:Vec::new(),
        }
    }

    pub fn consume(&mut self) -> Token {
        self.tokens.remove(0)
    }

    // fn peek(&self) -> Option<&Token> {
    //     self.tokens.get(0)
    // }

    pub fn tokenize(&mut self) {
        let chars:Vec<char> = self.input.chars().collect();
        let mut pos = 0;
        while pos < chars.len() {
            let current_char = chars[pos];
            if current_char.is_whitespace() {
                pos += 1;
                continue;
            }
            if current_char.is_alphanumeric() {
                let start = pos;
                while pos < chars.len() && chars[pos].is_alphanumeric() {
                    pos += 1;
                }
                let word:String = chars[start..pos].iter().collect();
                let upper_word = word.to_uppercase();
                let token = match upper_word.as_str() {
                    "CREATE" => Token::Keyword(KeyWords::CREATE),
                    "SELECT" => Token::Keyword(KeyWords::SELECT),
                    "INSERT" => Token::Keyword(KeyWords::INSERT),
                    "UPDATE" => Token::Keyword(KeyWords::UPDATE),
                    "STRING" => Token::Keyword(KeyWords::STRING),
                    "INT" => Token::Keyword(KeyWords::INT),
                    "FROM" => Token::Keyword(KeyWords::FROM),
                    "INTO" => Token::Keyword(KeyWords::INTO),
                    "VALUES"=>Token::Keyword(KeyWords::VALUES),
                    _ => Token::Identifier(word),
                };
                self.tokens.push(token);
                continue;
            }
            if current_char == ',' {
                self.tokens.push(Token::Keyword(KeyWords::COMMA));
                pos += 1;
                continue;
            } else if current_char == '(' {
                self.tokens.push(Token::Keyword(KeyWords::LEFTPAREN));
                pos += 1;
                continue;
            } else if current_char == ')' {
                self.tokens.push(Token::Keyword(KeyWords::RIGHTPAREN));
                pos += 1;
                continue;
            } else if current_char == '*' {
                self.tokens.push(Token::Identifier("*".to_string()));
                pos += 1;
                continue;
            }
            pos += 1;
        }
        self.tokens.push(Token::EOL);
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    fn next_token(lexer: &mut Lexer) -> Token {
        lexer.consume()
    }

    #[test]
    fn test_keywords_case_insensitive() {
        let mut lexer = Lexer::new("create SELECT Insert UpDaTe");
        lexer.tokenize();

        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::CREATE) => {}
            other => panic!("Expected CREATE, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::SELECT) => {}
            other => panic!("Expected SELECT, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::INSERT) => {}
            other => panic!("Expected INSERT, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::UPDATE) => {}
            other => panic!("Expected UPDATE, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::EOL => {}
            other => panic!("Expected EOL, got {:?}", other),
        }
    }

    #[test]
    fn test_identifiers_and_symbols() {
        let mut lexer = Lexer::new("CREATE table (id INT, name STRING)");
        lexer.tokenize();

        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::CREATE) => {}
            other => panic!("Expected CREATE, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Identifier(s) if s == "table" => {}
            other => panic!("Expected Identifier(table), got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::LEFTPAREN) => {}
            other => panic!("Expected LEFTPAREN, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Identifier(s) if s == "id" => {}
            other => panic!("Expected Identifier(id), got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::INT) => {}
            other => panic!("Expected INT, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::COMMA) => {}
            other => panic!("Expected COMMA, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Identifier(s) if s == "name" => {}
            other => panic!("Expected Identifier(name), got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::STRING) => {}
            other => panic!("Expected STRING, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::RIGHTPAREN) => {}
            other => panic!("Expected RIGHTPAREN, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::EOL => {}
            other => panic!("Expected EOL, got {:?}", other),
        }
    }

    #[test]
    fn test_symbols_only() {
        let mut lexer = Lexer::new(",()");
        lexer.tokenize();

        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::COMMA) => {}
            other => panic!("Expected COMMA, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::LEFTPAREN) => {}
            other => panic!("Expected LEFTPAREN, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::RIGHTPAREN) => {}
            other => panic!("Expected RIGHTPAREN, got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::EOL => {}
            other => panic!("Expected EOL, got {:?}", other),
        }
    }

    #[test]
    fn test_alphanumeric_identifier() {
        let mut lexer = Lexer::new("user1 col2a");
        lexer.tokenize();

        match next_token(&mut lexer) {
            Token::Identifier(s) if s == "user1" => {}
            other => panic!("Expected Identifier(user1), got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Identifier(s) if s == "col2a" => {}
            other => panic!("Expected Identifier(col2a), got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::EOL => {}
            other => panic!("Expected EOL, got {:?}", other),
        }
    }

    #[test]
    fn test_numbers_ignored_then_identifier() {
        // Leading numbers are ignored by current lexer; trailing alpha becomes an identifier.
        let mut lexer = Lexer::new("123 45a");
        lexer.tokenize();

        match next_token(&mut lexer) {
            // "123" is skipped entirely, then "45a" becomes "a"
            Token::Identifier(s) if s == "a" => {}
            other => panic!("Expected Identifier(a), got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::EOL => {}
            other => panic!("Expected EOL, got {:?}", other),
        }
    }

    #[test]
    fn test_select_star_from_identifier() {
        let mut lexer = Lexer::new("SELECT * from test");
        lexer.tokenize();

        match next_token(&mut lexer) {
            Token::Keyword(KeyWords::SELECT) => {}
            other => panic!("Expected SELECT, got {:?}", other),
        }
        match next_token(&mut lexer) {
            // Expect '*' to be tokenized as an identifier for now
            Token::Identifier(s) if s == "*" => {}
            other => panic!("Expected Identifier(*), got {:?}", other),
        }
        match next_token(&mut lexer) {
            // 'from' is not a keyword in current lexer, so it's an identifier
            Token::Keyword(KeyWords::FROM) => {}
            other => panic!("Expected Identifier(from), got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::Identifier(s) if s == "test" => {}
            other => panic!("Expected Identifier(test), got {:?}", other),
        }
        match next_token(&mut lexer) {
            Token::EOL => {}
            other => panic!("Expected EOL, got {:?}", other),
        }
    }
}
