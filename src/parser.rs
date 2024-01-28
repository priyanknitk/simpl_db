use crate::enums::PrepareResult;
use crate::row::Row;
use crate::statement::{Statement, StatementType};

pub fn prepare_statement(input: &str) -> PrepareResult {
    if input.starts_with("insert") {
        let parts = input.split_whitespace();
        // ignore the first part and parse the second, third, and fourth parts as id, username, and email
        let parts: Vec<&str> = parts.skip(1).collect();
        if parts.len() != 3 {
            eprintln!("Error parsing input.");
            return PrepareResult::PrepareSyntaxError;
        }
        match parts.as_slice() {
            [id_str, username, email] => {
                let id = match id_str.parse::<i32>() {
                    Ok(parsed_id) => parsed_id,
                    Err(_) => {
                        return PrepareResult::PrepareSyntaxError;
                    }
                };
                if id < 0 {
                    return PrepareResult::PrepareNegativeId;
                }
                return PrepareResult::PrepareSuccess(Statement {
                    statement_type: StatementType::StatementInsert,
                    row_to_insert: Some(Row {
                        id,
                        username: username.to_string(),
                        email: email.to_string(),
                    }),
                });
            }
            _ => {
                eprintln!("Error parsing input.");
                return PrepareResult::PrepareSyntaxError;
            }
        };
    } else if input.starts_with("select") {
        PrepareResult::PrepareSuccess(Statement {
            statement_type: StatementType::StatementSelect,
            row_to_insert: None,
        })
    } else {
        PrepareResult::PrepareUnrecognizedStatement
    }
}