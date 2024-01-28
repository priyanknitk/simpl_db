#[cfg(test)]
mod tests {
    use crate::{enums::ExecuteResult, executor, row::Row, statement::{self, Statement, StatementType}, table::Table};
 
    // Table tests
    #[test]
    fn table_open() {
        let table = Table::open("test.db");
        assert_eq!(table.root_page_num, 0, "root_page_num should be 0.");
        assert_eq!(table.pager.num_pages, 1, "num_pages should be 1.");
    }

    #[test]
    fn table_find() {
        let mut table = Table::open("test.db");
        let cursor = table.table_find(0);
        assert_eq!(cursor.cell_num, 0, "cell_num should be 0.");
        assert_eq!(cursor.page_num, 0, "page_num should be 0.");
        assert_eq!(cursor.end_of_table, false, "end_of_table should be false.");
    }

    #[test]
    fn table_start() {
        let mut table = Table::open("test.db");
        let cursor = table.table_start();
        assert_eq!(cursor.cell_num, 0, "cell_num should be 0.");
        assert_eq!(cursor.page_num, 0, "page_num should be 0.");
        assert_eq!(cursor.end_of_table, true, "end_of_table should be false.");
    }

    #[test]
    fn test_insert_row() {
        let (_, execute_result) =  insert_row();
        match execute_result {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            _ => panic!("Error executing statement.")
        }
    }

    #[test]
    fn select_test_row() {
        let (mut table, _) =  insert_row();
        let statement = Statement{
            row_to_insert: None,
            statement_type: StatementType::StatementSelect
        };
        let execute_result = executor::execute_statement(&statement, &mut table);
        match execute_result {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            _ => panic!("Error executing statement.")
        }
    }

    fn insert_row() -> (Table, ExecuteResult) {
        let mut table = Table::open("test.db");
        let row_to_insert = Row{
            id: 1,
            username: "test".to_string(),
            email: "test@test.com".to_string()
        };
        let statement = Statement{
            row_to_insert: Some(row_to_insert),
            statement_type: StatementType::StatementInsert
        };
        let execute_result = executor::execute_statement(&statement, &mut table);
        (table, execute_result)
    }
}