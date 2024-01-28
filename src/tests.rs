#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use pretty_assertions::{assert_eq, assert_ne};
    use crate::{
        enums::ExecuteResult,
        executor,
        row::Row,
        statement::{Statement, StatementType},
        table::Table,
    };

    const DEFAULT_DB_FILE_PATH: &str = "test.db";

    // Table tests
    #[test]
    fn table_open() {
        let table = open_table();
        assert_eq!(table.root_page_num, 0, "root_page_num should be 0.");
        assert_eq!(table.pager.num_pages, 1, "num_pages should be 1.");
    }

    #[test]
    fn table_find() {
        let mut table = open_table();
        let cursor = table.table_find(0);
        assert_eq!(cursor.cell_num, 0, "cell_num should be 0.");
        assert_eq!(cursor.page_num, 0, "page_num should be 0.");
        assert_eq!(cursor.end_of_table, false, "end_of_table should be false.");
    }

    #[test]
    fn table_start() {
        let mut table = open_table();
        let cursor = table.table_start();
        assert_eq!(cursor.cell_num, 0, "cell_num should be 0.");
        assert_eq!(cursor.page_num, 0, "page_num should be 0.");
        assert_eq!(cursor.end_of_table, true, "end_of_table should be false.");
    }

    #[test]
    fn test_insert_row() {
        let mut table = open_table();
        let (execute_result, _) = insert_row(&mut table);
        match execute_result {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            _ => panic!("Error executing statement."),
        }
    }

    #[test]
    fn select_test_row() {
        let mut table = open_table();
        let ( _, _) = insert_row(&mut table);
        let statement = Statement {
            row_to_insert: None,
            statement_type: StatementType::StatementSelect,
        };
        let execute_result = executor::execute_statement(&statement, &mut table);
        match execute_result {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            _ => panic!("Error executing statement."),
        }
    }

    #[test]
    fn test_inserted_data() {
        let mut table = open_table();
        let ( _, inserted_row) = insert_row(&mut table);
        let mut cursor = table.table_start();
        // the count of rows in the table should be 1
        assert_eq!(cursor.end_of_table, false, "end_of_table should be false.");
        let row = Row::deserialize_row(cursor.cursor_value());
        cursor.advance();
        assert_eq!(row, inserted_row, "row should match inserted row.");
        assert_eq!(cursor.end_of_table, true, "end_of_table should be true.");
    }

    #[test]
    fn test_node_splitting() {
        let mut table = Table::open("test.db");
        let mut inserted_rows: Vec<Row> = Vec::new();
        for i in 0..crate::constants::LEAF_NODE_MAX_CELLS + 1 {
            let row_to_insert = Row {
                id: i as i32,
                username: "test".to_string(),
                email: "test@test.com".to_string(),
            };
            let (execute_result, _) = insert_row_internal(&mut table, &row_to_insert);
            inserted_rows.push(row_to_insert);
            match execute_result {
                ExecuteResult::ExecuteSuccess => println!("Executed."),
                _ => panic!("Error executing statement."),
            }
        }

        let statement = Statement {
            row_to_insert: None,
            statement_type: StatementType::StatementSelect,
        };
        let execute_result = executor::execute_statement(&statement, &mut table);
        match execute_result {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            _ => panic!("Error executing statement."),
        }
        
        // Un-comment once recursive search is implemented
        // let mut cursor = table.table_start();
        // for i in 0..inserted_rows.len() {
        //     let row = Row::deserialize_row(cursor.cursor_value());
        //     assert_eq!(row, inserted_rows[i], "row should match inserted row.");
        //     cursor.advance();
        // }
    }

    #[test]
    fn test_node_splitting_insert_beginning() {
        let mut table = Table::open("test.db");
        let mut inserted_rows: Vec<Row> = Vec::new();
        for i in (0..crate::constants::LEAF_NODE_MAX_CELLS + 1).rev() {
            let row_to_insert = Row {
                id: i as i32,
                username: "test".to_string(),
                email: "test@test.com".to_string(),
            };
            let (execute_result, _) = insert_row_internal(&mut table, &row_to_insert);
            inserted_rows.push(row_to_insert);
            match execute_result {
                ExecuteResult::ExecuteSuccess => println!("Executed."),
                _ => panic!("Error executing statement."),
            }
        }
        let statement = Statement {
            row_to_insert: None,
            statement_type: StatementType::StatementSelect,
        };
        let execute_result = executor::execute_statement(&statement, &mut table);
        match execute_result {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            _ => panic!("Error executing statement."),
        }

        // Un-comment once recursive search is implemented
        // let mut cursor = table.table_start();
        // for i in 0..inserted_rows.len() {
        //     let row = Row::deserialize_row(cursor.cursor_value());
        //     assert_eq!(row, inserted_rows[i], "row should match inserted row.");
        //     cursor.advance();
        // }
    }

    #[test]
    fn test_node_splitting_insert_middle() {
        let mut table = Table::open("test.db");
        let mut inserted_rows: Vec<Row> = Vec::new();

        // randomly pick a value from 0 to LEAF_NODE_MAX_CELLS, LEAF_NODE_MAX_CELLS times
        let mut rng = rand::thread_rng();
        let mut random_values: Vec<usize> = (0..crate::constants::LEAF_NODE_MAX_CELLS + 1).collect();
        random_values.shuffle(&mut rng);

        for key in random_values {
            let row_to_insert = Row {
                id: key as i32,
                username: "test".to_string(),
                email: "".to_string(),
            };
            let (execute_result, _) = insert_row_internal(&mut table, &row_to_insert);
            inserted_rows.push(row_to_insert);
            match execute_result {
                ExecuteResult::ExecuteSuccess => println!("Executed."),
                _ => panic!("Error executing statement."),
            }
        }
        let statement = Statement {
            row_to_insert: None,
            statement_type: StatementType::StatementSelect,
        };
        let execute_result = executor::execute_statement(&statement, &mut table);
        match execute_result {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            _ => panic!("Error executing statement."),
        }

        // Un-comment once recursive search is implemented
        // let mut cursor = table.table_start();
        // for i in 0..inserted_rows.len() {
        //     let row = Row::deserialize_row(cursor.cursor_value());
        //     assert_eq!(row, inserted_rows[i], "row should match inserted row.");
        //     cursor.advance();
        // }
    }

    // Helper functions

    fn open_table() -> Table {
        Table::open(DEFAULT_DB_FILE_PATH)
    }

    fn insert_row(table: &mut Table) -> (ExecuteResult, Row) {
        let row_to_insert = Row {
            id: 1,
            username: "test".to_string(),
            email: "test@test.com".to_string(),
        };
        insert_row_internal(table, &row_to_insert)
    }

    fn insert_row_internal(table: &mut Table, row_to_insert: &Row) -> (ExecuteResult, Row) {
        let cloned_row = row_to_insert.clone();
        let statement = Statement {
            row_to_insert: Some(row_to_insert.clone()),
            statement_type: StatementType::StatementInsert,
        };
        let execute_result = executor::execute_statement(&statement, table);
        (execute_result, cloned_row)
    }
}
