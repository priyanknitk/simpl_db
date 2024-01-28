#[cfg(test)]
mod tests {
    use crate::table::Table;
 
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
}