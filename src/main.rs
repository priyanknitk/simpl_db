use enums::{ExecuteResult, MetaCommandResult, PrepareResult};
use std::io;
use std::io::Write;
use table::Table;

mod constants;
mod cursor;
mod enums;
mod executor;
mod node;
mod pager;
mod row;
mod statement;
mod table;
mod parser;

#[cfg(test)]
mod tests;

const DEFAULT_DB_FILE_PATH: &str = ".\\myDb.db";

fn main() {
    let mut db_file_path: String = String::new();
    std::env::args().skip(1).next().map(|arg| {
        db_file_path = arg;
    }).unwrap_or_else(|| {
        db_file_path = DEFAULT_DB_FILE_PATH.to_string();
    });
    let mut table = Table::open(&db_file_path);
    loop {
        print_prompt();
        let input = read_input();
        if input.starts_with(".") {
            match do_meta_command(&input, &mut table) {
                MetaCommandResult::MetaCommandSuccess => continue,
                MetaCommandResult::MetaCommandUnrecognizedCommand => {
                    eprintln!("Unrecognized command '{}'.", input);
                    continue;
                }
            }
        }
        let statement = match parser::prepare_statement(&input) {
            PrepareResult::PrepareSuccess(parsed_statement) => parsed_statement,
            PrepareResult::PrepareSyntaxError => {
                eprintln!("Syntax error. Could not parse statement '{}'.", input);
                continue;
            }
            PrepareResult::PrepareUnrecognizedStatement => {
                eprintln!("Unrecognized keyword at start of '{}'.", input);
                continue;
            }
            PrepareResult::PrepareNegativeId => {
                eprintln!("ID must be positive.");
                continue;
            }
        };

        match executor::execute_statement(&statement, &mut table) {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            ExecuteResult::ExecuteTableDuplicateKey => println!("Error: Duplicate key."),
            ExecuteResult::ExecuteTableFull => println!("Error: Table full."),
        }
    }
}

fn do_meta_command(input: &str, table: &mut Table) -> MetaCommandResult {
    if input == ".exit" {
        table.db_close();
        std::process::exit(0);
    } else {
        MetaCommandResult::MetaCommandUnrecognizedCommand
    }
}


fn read_input() -> String {
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap_or_else(|err| {
        eprintln!("Error reading input: {}", err);
        panic!("Error reading input.");
    });
    input.trim().to_string()
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap_or_else(|err| {
        eprintln!("Error flushing stdout: {}", err);
        panic!("Error flushing stdout.");
    });
}
