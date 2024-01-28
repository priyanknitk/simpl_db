use crate::table::Table;
use crate::node::{leaf_node_num_cells, leaf_node_value};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub page_num: usize,
    pub cell_num: usize,
    pub end_of_table: bool,
}

impl Cursor<'_> {
    pub fn advance(&mut self) {
        let page = self.table.pager.get_page(self.page_num);
        self.cell_num += 1;
        if self.cell_num >= leaf_node_num_cells(page) as usize {
            self.end_of_table = true;
        }
    }

    pub fn cursor_value(&mut self) -> &mut [u8] {
        let page = self.table.pager.get_page(self.page_num);
        leaf_node_value(page, self.cell_num)
    }
}