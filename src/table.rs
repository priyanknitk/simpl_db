use crate::constants::PAGE_SIZE;
use crate::pager::Pager;
use crate::cursor::Cursor;
use crate::enums::NodeType;
use crate::node::{get_node_type, initialize_leaf_node, internal_node_child, internal_node_key, internal_node_num_keys, leaf_node_key, leaf_node_num_cells, set_node_root};

pub struct Table {
    pub root_page_num: usize,
    pub pager: Box<Pager>,
}

impl Table {
    pub fn open(file_name: &str) -> Table {
        let mut pager = Pager::open(file_name);
        let root_page_num = 0;
        if pager.num_pages == 0 {
            // new database file
            let root_node = pager.get_page(root_page_num);
            initialize_leaf_node(root_node);
            set_node_root(root_node, true);
        }
        Table {
            root_page_num,
            pager: Box::new(pager),
        }
    }

    pub fn table_find(&mut self, key: u32) -> Cursor {
        let root_page_num = self.root_page_num;
        let root_node = self.pager.get_page(root_page_num);
        let node_type = get_node_type(root_node);
        match node_type {
            NodeType::NodeLeaf => self.leaf_node_find(root_page_num, key),
            NodeType::NodeInternal => self.internal_node_find(root_page_num, key),
        }
    }

    pub fn leaf_node_find(&mut self, page_num: usize, key: u32) -> Cursor {
        fn binary_search_leaf(root_node: &mut [u8], key: u32, num_cells: u32) -> usize {
            let mut min_index = 0;
            let mut one_past_max_index = num_cells as usize;
            while one_past_max_index != min_index {
                let index = (min_index + one_past_max_index) / 2;
                let key_at_index =
                    u32::from_le_bytes(leaf_node_key(root_node, index).try_into().unwrap());
                if key == key_at_index {
                    return index;
                }
                if key < key_at_index {
                    one_past_max_index = index;
                } else {
                    min_index = index + 1;
                }
            }
            min_index
        }

        let node = self.pager.get_page(page_num);
        let num_cells = leaf_node_num_cells(node);
        let cell_num = binary_search_leaf(node, key, num_cells);
        Cursor {
            table: self,
            page_num,
            cell_num,
            end_of_table: false,
        }
    }

    pub fn internal_node_find_child(&mut self, root_node: &mut [u8], key: u32, num_keys: u32) -> usize {
        let mut min_index = 0;
        let mut max_index = num_keys as usize;
        while max_index != min_index {
            let index: usize = (min_index + max_index) / 2;
            let key_to_right = u32::from_le_bytes(
                internal_node_key(root_node, index).try_into().unwrap(),
            );
            if key_to_right >= key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        min_index
    }

    pub fn internal_node_find(&mut self, page_num: usize, key: u32) -> Cursor {
        let node = self.pager.get_page(page_num);
        let mut node_buffer = [0u8; PAGE_SIZE];
        node_buffer.copy_from_slice(node);
        let num_keys = internal_node_num_keys(&mut node_buffer);
        let child_index = self.internal_node_find_child(&mut node_buffer, key, num_keys);
        let child_num = usize::from_le_bytes(
            internal_node_child(&mut node_buffer, child_index).try_into().unwrap(),
        ) as usize;
        let child_node = self.pager.get_page(child_num);
        match get_node_type(child_node) {
            NodeType::NodeInternal => self.internal_node_find(child_num, key),
            NodeType::NodeLeaf => self.leaf_node_find(child_num, key),
        }
    }

    pub fn table_start(&mut self) -> Cursor {
        let page_num = self.get_page_num_for_key(0);
        let node = self.pager.get_page(page_num);
        let num_cells = leaf_node_num_cells(node);
        let mut cursor = self.table_find(0);
        cursor.end_of_table = num_cells == 0;
        cursor
    }

    pub fn db_close(&mut self) {
        for i in 0..self.pager.num_pages {
            if self.pager.pages[i].is_none() {
                continue;
            }
            self.pager.pager_flush(i);
        }
    }

    fn get_page_num_for_key(&mut self, key: u32) -> usize {
        let cursor = self.table_find(key);
        cursor.page_num
    }
}
