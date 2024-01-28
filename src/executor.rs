use crate::enums::ExecuteResult;
use crate::node;
use crate::row::Row;
use crate::statement::{Statement, StatementType};
use crate::table::Table;

pub fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult {
    match statement.statement_type {
        StatementType::StatementInsert => execute_insert(statement, table),
        StatementType::StatementSelect => execute_select(statement, table),
    }
}

fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
    let row_to_insert = statement.row_to_insert.as_ref().unwrap();
    let mut cursor = table.table_find(row_to_insert.id as u32);
    node::leaf_node_insert(&mut cursor, row_to_insert.id as u32, row_to_insert);
    ExecuteResult::ExecuteSuccess
}

fn execute_select(_statement: &Statement, table: &mut Table) -> ExecuteResult {
    let mut cursor = table.table_start();
    while cursor.end_of_table == false {
        let row = Row::deserialize_row(cursor.cursor_value());
        println!("({}, {}, {})", row.id, row.username, row.email);
        cursor.advance();
    }
    ExecuteResult::ExecuteSuccess
}