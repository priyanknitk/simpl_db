use crate::table::Table;
use crate::node::{leaf_node_num_cells, leaf_node_value, leaf_node_next_leaf};

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
            let next_node_num = leaf_node_next_leaf(page) as usize;
            if next_node_num == 0 {
                self.end_of_table = true;
            } else {
                self.page_num = next_node_num;
                self.cell_num = 0;
            }
        }
    }

    pub fn cursor_value(&mut self) -> &mut [u8] {
        let page = self.table.pager.get_page(self.page_num);
        leaf_node_value(page, self.cell_num)
    }
}