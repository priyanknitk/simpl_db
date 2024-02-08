use crate::{constants::*, cursor};
use crate::cursor::Cursor;
use crate::enums::NodeType;
use crate::row::Row;
use crate::table::Table;

pub fn leaf_node_num_cells(node: &[u8]) -> u32 {
    let num_cells_slice =
        &node[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE];
    u32::from_le_bytes(num_cells_slice.try_into().unwrap())
}

fn set_leaf_node_num_cells(node: &mut [u8], num_cells: u32) {
    let num_cells_slice =
        node[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE].as_mut();
    num_cells_slice.copy_from_slice(&num_cells.to_le_bytes());
}

pub fn leaf_node_cell_unmut(node: &[u8], cell_num: usize) -> &[u8] {
    let num_cells = leaf_node_num_cells(node);
    if cell_num as u32 > num_cells {
        eprintln!("Tried to access cell {} > {}.", cell_num, num_cells);
        panic!("Tried to access cell out of bounds.");
    }
    let cell_offset = LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
    &node[cell_offset..cell_offset + LEAF_NODE_CELL_SIZE]
}

pub fn leaf_node_cell(node: &[u8], cell_num: usize) -> &[u8] {
    let num_cells = leaf_node_num_cells(node);
    if cell_num as u32 > num_cells {
        eprintln!("Tried to access cell {} > {}.", cell_num, num_cells);
        panic!("Tried to access cell out of bounds.");
    }
    let cell_offset = LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
    &node[cell_offset..cell_offset + LEAF_NODE_CELL_SIZE]
}

pub fn leaf_node_cell_mut(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    let num_cells = leaf_node_num_cells(node);
    if cell_num as u32 > num_cells {
        eprintln!("Tried to access cell {} > {}.", cell_num, num_cells);
        panic!("Tried to access cell out of bounds.");
    }
    let cell_offset = LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
    node[cell_offset..cell_offset + LEAF_NODE_CELL_SIZE].as_mut()
}

pub fn leaf_node_key(node: &[u8], cell_num: usize) -> &[u8] {
    let cell = leaf_node_cell(node, cell_num);
    &cell[LEAF_NODE_KEY_OFFSET..LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE]
}

pub fn leaf_node_value(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    let cell = leaf_node_cell_mut(node, cell_num);
    cell[LEAF_NODE_KEY_SIZE..LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE].as_mut()
}

pub fn initialize_leaf_node(node: &mut [u8]) {
    set_leaf_node_num_cells(node, 0);
    set_node_type(node, NodeType::NodeLeaf);
    set_node_root(node, false);
    set_next_leaf(node, 0);
}

pub fn initialize_internal_node(node: &mut [u8]) {
    set_node_type(node, NodeType::NodeInternal);
    set_node_root(node, false);
    // initialize the right child page number to an invalid page number
    set_internal_node_right_child(node, INVALID_PAGE_NUMBER);
}

pub fn leaf_node_insert(cursor: &mut Cursor, key: u32, row_to_insert: &Row) {
    let node = cursor.table.pager.get_page(cursor.page_num);
    let num_cells = leaf_node_num_cells(node);
    if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
        leaf_node_split_and_insert(cursor, key, row_to_insert);
        return;
    }
    if cursor.cell_num < num_cells as usize {
        // make room for the new cell
        for i in (cursor.cell_num..num_cells as usize).rev() {
            let destination_offset = LEAF_NODE_HEADER_SIZE + (i + 1) * LEAF_NODE_CELL_SIZE;
            let source_offset = LEAF_NODE_HEADER_SIZE + i * LEAF_NODE_CELL_SIZE;
            let cloned_slice = node[source_offset..source_offset + LEAF_NODE_CELL_SIZE].to_vec();
            node[destination_offset..destination_offset + LEAF_NODE_CELL_SIZE]
                .copy_from_slice(&cloned_slice);
        }
    }
    // insert the new cell
    set_leaf_node_num_cells(node, num_cells + 1);
    let destination_cell = leaf_node_cell_mut(node, cursor.cell_num);
    row_to_insert.serialize_row(destination_cell);
}

pub fn get_node_type(node: &[u8]) -> NodeType {
    let node_type_slice = &node[NODE_TYPE_OFFSET..NODE_TYPE_OFFSET + NODE_TYPE_SIZE];
    let node_type = u8::from_le_bytes(node_type_slice.try_into().unwrap());
    match node_type {
        0 => NodeType::NodeInternal,
        1 => NodeType::NodeLeaf,
        _ => panic!("Unknown node type."),
    }
}

pub fn set_node_type(node: &mut [u8], node_type: NodeType) {
    let node_type_slice = &mut node[NODE_TYPE_OFFSET..NODE_TYPE_OFFSET + NODE_TYPE_SIZE];
    match node_type {
        NodeType::NodeInternal => node_type_slice.copy_from_slice(&[0]),
        NodeType::NodeLeaf => node_type_slice.copy_from_slice(&[1]),
    }
}

pub fn leaf_node_split_and_insert(cursor: &mut Cursor, _key: u32, row_to_insert: &Row) {
    let new_page_num = cursor.table.pager.get_unused_page_num();
    let new_node_buffer = &mut [b'\0'; PAGE_SIZE][..];
    let old_node = cursor.table.pager.get_page_unmut(cursor.page_num);

    // copy old node to a temporary buffer
    let old_node_buffer = &mut [b'\0'; PAGE_SIZE];
    old_node_buffer.copy_from_slice(old_node);

    let old_max_key = get_node_max_key(cursor.table, old_node_buffer);

    // All existing keys plus new key should be divided
    // evenly between old (left) and new (right) nodes.
    // Starting from the right, move each key to correct position.
    for i in (0..LEAF_NODE_MAX_CELLS + 1).rev() {
        let destination_node: &mut [u8];
        if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
            destination_node = new_node_buffer;
        } else {
            destination_node = old_node_buffer;
        }
        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        let destination = leaf_node_cell_mut(destination_node, index_within_node);
        if i == cursor.cell_num {
            // copy key and value to the new node
            let destination_cell = leaf_node_cell_mut(destination_node, index_within_node);
            row_to_insert.serialize_row(destination_cell);
        } else if i > cursor.cell_num {
            let source = leaf_node_cell_unmut(old_node, i - 1);
            destination.copy_from_slice(source);
        } else {
            let source = leaf_node_cell_unmut(old_node, i);
            destination.copy_from_slice(source);
        }
    }

    set_node_parent(new_node_buffer, get_node_parent(old_node_buffer));
    set_next_leaf(new_node_buffer, leaf_node_next_leaf(old_node_buffer));
    set_next_leaf(old_node_buffer, new_page_num as u32);

    let new_node = cursor.table.pager.get_page(new_page_num);
    // copy new_node_buffer to new_node
    new_node.copy_from_slice(new_node_buffer);

    initialize_leaf_node(new_node);

    // Update cell count on both leaf nodes
    let old_node_num_cells = LEAF_NODE_LEFT_SPLIT_COUNT as u32;
    let new_node_num_cells = LEAF_NODE_RIGHT_SPLIT_COUNT as u32;

    set_leaf_node_num_cells(new_node, new_node_num_cells);
    set_leaf_node_num_cells(old_node_buffer, old_node_num_cells);

    let old_node = cursor.table.pager.get_page(cursor.page_num);

    // copy old_node_buffer to old_node
    old_node.copy_from_slice(old_node_buffer);

    if is_node_root(old_node) {
        return create_new_root(cursor.table, new_page_num);
    } else {
        let parent_page_num = get_node_parent(old_node);
        let new_max_key = get_node_max_key(cursor.table, old_node_buffer);
        let parent_node = cursor.table.pager.get_page(parent_page_num as usize);
        let parent_node_buffer = &mut [b'\0'; PAGE_SIZE];
        parent_node_buffer.copy_from_slice(parent_node);

        let num_keys = internal_node_num_keys(parent_node_buffer);

        let old_child_num =
            cursor
                .table
                .internal_node_find_child(parent_node_buffer, old_max_key, num_keys);
        let parent_node = cursor.table.pager.get_page(parent_page_num as usize);
        update_internal_node_key(parent_node, new_max_key, old_child_num);
        internal_node_insert(cursor.table, parent_page_num as usize, new_page_num);
    }
}

fn is_node_root(node: &[u8]) -> bool {
    let is_root_slice = &node[IS_ROOT_OFFSET..IS_ROOT_OFFSET + IS_ROOT_SIZE];
    let is_root = u8::from_le_bytes(is_root_slice.try_into().unwrap());
    match is_root {
        0 => false,
        1 => true,
        _ => panic!("Unknown value for is_root."),
    }
}

pub fn set_node_root(node: &mut [u8], is_root: bool) {
    let is_root_slice = &mut node[IS_ROOT_OFFSET..IS_ROOT_OFFSET + IS_ROOT_SIZE];
    match is_root {
        false => is_root_slice.copy_from_slice(&[0]),
        true => is_root_slice.copy_from_slice(&[1]),
    }
}

fn set_next_leaf(node: &mut [u8], next_leaf: u32) {
    let next_leaf_slice = &mut node
        [LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE];
    next_leaf_slice.copy_from_slice(&next_leaf.to_le_bytes());
}

fn create_new_root(table: &mut Table, right_child_page_num: usize) {
    // populating the right child page in the pager
    let _right_child = table.pager.get_page(right_child_page_num);
    
    let left_child_page_num = table.pager.get_unused_page_num();

    let root = table.pager.get_page(table.root_page_num);

    // create a buffer to hold the old root node
    let new_root_buffer = &mut [b'\0'; PAGE_SIZE];
    new_root_buffer.copy_from_slice(root);

    let new_left_child_buffer = &mut [b'\0'; PAGE_SIZE];

    match get_node_type(&root) {
        NodeType::NodeInternal => {
            initialize_internal_node(table.pager.get_page(right_child_page_num));
            initialize_internal_node(new_left_child_buffer);
        }
        NodeType::NodeLeaf => (),
    }

    new_left_child_buffer.copy_from_slice(new_root_buffer);
    set_node_root(new_left_child_buffer, false);

    match get_node_type(new_left_child_buffer) {
        NodeType::NodeInternal => {
            let mut child_node: &mut [u8];
            let left_child_num_keys = internal_node_num_keys(new_left_child_buffer);
            for i in 0..left_child_num_keys {
                child_node = table.pager.get_page(
                    internal_node_child_page_num(new_left_child_buffer, i as usize));
                set_node_parent(child_node, left_child_page_num as u32);
            }

            child_node = table.pager.get_page(
                usize::from_le_bytes(internal_node_right_child(new_left_child_buffer).try_into().unwrap()),
            );
            set_node_parent(child_node, left_child_page_num as u32);
        }
        NodeType::NodeLeaf => (),
    }

    initialize_internal_node(new_root_buffer);
    set_node_root(new_root_buffer, true);
    let num_keys_root_value: u32 = 1;
    
    set_internal_node_num_keys(new_root_buffer, num_keys_root_value);
    set_internal_node_child(new_root_buffer, left_child_page_num, 0);
    let left_child_max_key = get_node_max_key(table, new_left_child_buffer);
    set_internal_node_key(new_root_buffer, left_child_max_key, 0);
    set_internal_node_right_child(new_root_buffer, right_child_page_num);

    // copy the buffers back to the pages
    let root = table.pager.get_page(table.root_page_num);
    root.copy_from_slice(new_root_buffer);

    let left_child = table.pager.get_page(left_child_page_num);
    left_child.copy_from_slice(new_left_child_buffer);
    set_node_parent(left_child, table.root_page_num as u32);
    let right_child = table.pager.get_page(right_child_page_num);
    set_node_parent(right_child, table.root_page_num as u32);
}

fn internal_node_insert(table: &mut Table, parent_page_num: usize, child_page_num: usize) {
    let parent = table.pager.get_page_unmut(parent_page_num);
    let parent_node_num_keys = internal_node_num_keys(parent);
    let parent_node_buffer = &mut [b'\0'; PAGE_SIZE];
    parent_node_buffer.copy_from_slice(parent);
    let child = table.pager.get_page_unmut(child_page_num);
    let child_max_key = get_node_max_key(table, child);
    let index =
        table.internal_node_find_child(parent_node_buffer, child_max_key, parent_node_num_keys);

    if parent_node_num_keys >= INTERNAL_NODE_MAX_CELLS as u32 {
        internal_node_split_and_insert(table, parent_page_num, child_page_num);
        return;
    }

    let right_child_page_num = usize::from_le_bytes(
        internal_node_right_child(parent_node_buffer)
            .try_into()
            .unwrap(),
    );
    // if the right child is invalid, just insert the new child
    if right_child_page_num == INVALID_PAGE_NUMBER {
        set_internal_node_right_child(parent_node_buffer, child_page_num);
        // copy the buffer back to the page
        let parent = table.pager.get_page(parent_page_num);
        parent.copy_from_slice(parent_node_buffer);
        return;
    }

    let right_child = table.pager.get_page(right_child_page_num);

    // if we are already at the maximum number of cells, we can't increment before splitting.
    // Incrementing without inserting a new key/child pair and immediately splitting has the effect of
    // creating a new key at max_cells + 1, with an uninitialized value.
    increment_internal_node_num_keys(parent_node_buffer);

    let right_child_buffer = &mut [b'\0'; PAGE_SIZE];
    right_child_buffer.copy_from_slice(right_child);  

    if child_max_key > get_node_max_key(table, right_child_buffer) {
        // Replace right child
        set_internal_node_child(parent_node_buffer, right_child_page_num, parent_node_num_keys as usize,);
        set_internal_node_key(parent_node_buffer, get_node_max_key(table, right_child_buffer), parent_node_num_keys as usize);
        set_internal_node_right_child(parent_node_buffer, child_page_num);
    } else {
        // Make room for the new cell
        for i in (index..parent_node_num_keys as usize).rev() {
            let destination_offset = INTERNAL_NODE_HEADER_SIZE + (i + 1) * INTERNAL_NODE_CELL_SIZE;
            let source_offset = INTERNAL_NODE_HEADER_SIZE + i * INTERNAL_NODE_CELL_SIZE;
            let cloned_slice =
                parent_node_buffer[source_offset..source_offset + INTERNAL_NODE_CELL_SIZE].to_vec();
                parent_node_buffer[destination_offset..destination_offset + INTERNAL_NODE_CELL_SIZE]
                .copy_from_slice(&cloned_slice);
        }
        set_internal_node_child(parent_node_buffer, child_page_num, index);
        set_internal_node_key(parent_node_buffer, child_max_key, index);
    }

    // copy the buffer back to the page
    let parent = table.pager.get_page(parent_page_num);
    parent.copy_from_slice(parent_node_buffer);

    let right_child = table.pager.get_page(right_child_page_num);
    right_child.copy_from_slice(right_child_buffer);
}

fn internal_node_split_and_insert(
    table: &mut Table,
    parent_page_num: usize,
    child_page_num: usize,
) {
    let mut old_page_num = parent_page_num;
    let mut old_node = table.pager.get_page_unmut(old_page_num);
    let old_max = get_node_max_key(table, old_node);

    let old_node_buffer = &mut [b'\0'; PAGE_SIZE];
    old_node_buffer.copy_from_slice(old_node);

    let child_node = table.pager.get_page_unmut(child_page_num);
    let child_max = get_node_max_key(table, child_node);

    let new_page_num = table.pager.get_unused_page_num();

    let splitting_root = is_node_root(old_node_buffer);

    let parent_node: &mut [u8];
    let parent_node_buffer = &mut [b'\0'; PAGE_SIZE];
    let new_node: &mut [u8];
    let new_node_buffer = &mut [b'\0'; PAGE_SIZE];
    if splitting_root {
        create_new_root(table, new_page_num);
        new_node_buffer.copy_from_slice(table.pager.get_page(new_page_num));
        parent_node = table.pager.get_page(table.root_page_num);
        parent_node_buffer.copy_from_slice(parent_node);
        old_page_num =
            usize::from_le_bytes(internal_node_child(parent_node, 0).try_into().unwrap());
        old_node = table.pager.get_page(old_page_num);
        old_node_buffer.copy_from_slice(old_node);
    } else {
        let old_node_parent = get_node_parent(old_node);
        parent_node = table.pager.get_page(old_node_parent as usize);
        parent_node_buffer.copy_from_slice(parent_node);
        new_node = table.pager.get_page(new_page_num);
        initialize_internal_node(new_node);
        new_node_buffer.copy_from_slice(new_node);
    }

    let mut cur_page_num = usize::from_le_bytes(
        internal_node_right_child_unmut(old_node_buffer)
            .try_into()
            .unwrap(),
    );
    // First put the right child in the new node and set right child to invalid
    internal_node_insert(table, new_page_num, cur_page_num);
    new_node_buffer.copy_from_slice(table.pager.get_page(new_page_num));

    let mut cur = table.pager.get_page(cur_page_num);
    set_node_parent(cur, new_page_num as u32);
    let cur_node_buffer = &mut [b'\0'; PAGE_SIZE];
    cur_node_buffer.copy_from_slice(cur);
    set_internal_node_right_child(old_node_buffer, INVALID_PAGE_NUMBER);

    // For each key until you get the middle key, move the key and child to the new node
    for i in ((INTERNAL_NODE_MAX_CELLS / 2 + 1)..(INTERNAL_NODE_MAX_CELLS)).rev() {
        cur_page_num = usize::from_le_bytes(internal_node_child(old_node_buffer, i).try_into().unwrap());
        internal_node_insert(table, new_page_num, cur_page_num);
        cur = table.pager.get_page(cur_page_num);
        cur_node_buffer.copy_from_slice(cur);
        set_node_parent(cur_node_buffer, new_page_num as u32);
        decrement_internal_node_num_keys(old_node_buffer);
        let cur = table.pager.get_page(cur_page_num);
        cur.copy_from_slice(cur_node_buffer);
    }

    new_node_buffer.copy_from_slice(table.pager.get_page(new_page_num));

    // Set child before middle key which is now the highest key to be node's right child
    // and decrement the number of keys
    let old_num_keys = internal_node_num_keys(old_node_buffer);
    let old_node_child = internal_node_child(old_node_buffer, (old_num_keys - 1) as usize);
    let old_node_child_num = usize::from_le_bytes(old_node_child.try_into().unwrap());

    set_internal_node_right_child(old_node_buffer, old_node_child_num);

    decrement_internal_node_num_keys(old_node_buffer);

    // Determine which of the two nodes to insert into
    let max_after_split = get_node_max_key(table, old_node_buffer);
    let destination_page_num = if child_max < max_after_split {
        old_page_num
    } else {
        new_page_num
    };
    internal_node_insert(table, destination_page_num, child_page_num);
    new_node_buffer.copy_from_slice(table.pager.get_page(new_page_num));
    let child_node = table.pager.get_page(child_page_num);
    set_node_parent(child_node, destination_page_num as u32);

    let old_num_keys = internal_node_num_keys(old_node_buffer);
    let old_child_num = table.internal_node_find_child(parent_node_buffer, old_max, old_num_keys);

    update_internal_node_key(
        parent_node_buffer,
        get_node_max_key(table, old_node_buffer),
        old_child_num,
    );

    if !splitting_root {
        internal_node_insert(table, get_node_parent(old_node_buffer) as usize, new_page_num);
        new_node_buffer.copy_from_slice(table.pager.get_page(new_page_num));
        set_node_parent(new_node_buffer, get_node_parent(old_node_buffer));
    }

    // copy the buffer back to the page
    let parent = table.pager.get_page(parent_page_num);
    parent.copy_from_slice(parent_node_buffer);

    let old_node = table.pager.get_page(old_page_num);
    old_node.copy_from_slice(old_node_buffer);

    let new_node = table.pager.get_page(new_page_num);
    new_node.copy_from_slice(new_node_buffer);
}

pub fn internal_node_num_keys(node: &[u8]) -> u32 {
    let num_keys_slice = &node[INTERNAL_NODE_NUM_KEYS_OFFSET
        ..INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE];
    u32::from_le_bytes(num_keys_slice.try_into().unwrap())
}

fn increment_internal_node_num_keys(node: &mut [u8]) {
    let num_keys = internal_node_num_keys(node);
    set_internal_node_num_keys(node, num_keys + 1);
}

fn decrement_internal_node_num_keys(node: &mut [u8]) {
    let num_keys = internal_node_num_keys(node);
    set_internal_node_num_keys(node, num_keys - 1);
}

pub fn set_internal_node_num_keys(node: &mut [u8], num_keys: u32) {
    node[INTERNAL_NODE_NUM_KEYS_OFFSET..INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE]
        .as_mut().copy_from_slice(&num_keys.to_le_bytes());
}

pub fn set_internal_node_right_child(node: &mut [u8], right_child_page_num: usize) {
    internal_node_right_child(node).copy_from_slice(&right_child_page_num.to_le_bytes());
}

fn internal_node_right_child(node: &mut [u8]) -> &mut [u8] {
    node[INTERNAL_NODE_RIGHT_CHILD_OFFSET
        ..INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE]
        .as_mut()
}

fn internal_node_right_child_unmut(node: &[u8]) -> &[u8] {
    node[INTERNAL_NODE_RIGHT_CHILD_OFFSET
        ..INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE]
        .as_ref()
}

fn internal_node_cell(node: &[u8], cell_num: usize) -> &[u8] {
    let cell_offset = INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
    &node[cell_offset..cell_offset + INTERNAL_NODE_CELL_SIZE]
}

fn internal_node_cell_mut(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    let cell_offset = INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
    &mut node[cell_offset..cell_offset + INTERNAL_NODE_CELL_SIZE]
}

fn internal_node_child_page_num(node: &mut [u8], cell_num: usize) -> usize {
    usize::from_le_bytes(internal_node_cell_mut(node, cell_num)[..INTERNAL_NODE_CHILD_SIZE].try_into().unwrap())
}

pub fn internal_node_key(node: &[u8], cell_num: usize) -> &[u8] {
    let cell = internal_node_cell(node, cell_num);
    &cell[INTERNAL_NODE_CHILD_SIZE..INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE]
}

fn set_internal_node_key(node: &mut [u8], key: u32, cell_num: usize) {
    let cell = internal_node_cell_mut(node, cell_num);
    cell[INTERNAL_NODE_CHILD_SIZE..INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE].copy_from_slice(&key.to_le_bytes());
}

fn set_internal_node_child(node: &mut [u8], child_page_num: usize, child_num: usize) {
    let node_child = internal_node_child(node, child_num);
    node_child.copy_from_slice(&child_page_num.to_le_bytes());
}

pub fn internal_node_child(node: &mut [u8], child_num: usize) -> &mut [u8] {
    let num_keys = internal_node_num_keys(node);
    if child_num as u32 > num_keys {
        eprintln!("Tried to access child {} > {}.", child_num, num_keys);
        panic!("Tried to access child out of bounds.");
    } else if child_num as u32 == num_keys {
        let right_child_page_num =
            usize::from_le_bytes(internal_node_right_child_unmut(node).try_into().unwrap());
        if right_child_page_num == INVALID_PAGE_NUMBER {
            eprintln!("Tried to access child {} > {}.", child_num, num_keys);
            panic!("Tried to access child out of bounds.");
        }
        internal_node_right_child(node)
    } else {
        let child_page_num = internal_node_child_page_num(node, child_num);
        let child_node = internal_node_cell_mut(node, child_num);
        if child_page_num == INVALID_PAGE_NUMBER {
            eprintln!("Tried to access child {} > {}.", child_num, num_keys);
            panic!("Tried to access child out of bounds.");
        }
        child_node[..INTERNAL_NODE_CHILD_SIZE].as_mut()
    }
}

pub fn leaf_node_next_leaf(node: &[u8]) -> u32 {
    u32::from_le_bytes(
        node[LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE]
            .try_into()
            .unwrap(),
    )
}

pub fn get_node_max_key(table: &Table, node: &[u8]) -> u32 {
    match get_node_type(node) {
        NodeType::NodeInternal => {
            let right_child_page_num =
                usize::from_le_bytes(internal_node_right_child_unmut(node).try_into().unwrap());
            let right_child = table.pager.get_page_unmut(right_child_page_num);
            get_node_max_key(table, right_child)
        }
        NodeType::NodeLeaf => {
            let num_cells = leaf_node_num_cells(node);
            let max_key = u32::from_le_bytes(
                leaf_node_key(node, num_cells as usize - 1)
                    .try_into()
                    .unwrap(),
            );
            max_key
        }
    }
}

pub fn get_node_parent(node: &[u8]) -> u32 {
    let parent_pointer_slice =
        &node[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE];
    u32::from_le_bytes(parent_pointer_slice.try_into().unwrap())
}

pub fn set_node_parent(node: &mut [u8], parent: u32) {
    let parent_pointer_slice =
        &mut node[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE];
    parent_pointer_slice.copy_from_slice(&parent.to_le_bytes());
}

pub fn update_internal_node_key(node: &mut [u8], new_key: u32, old_child_num: usize) {
    let cell = internal_node_cell_mut(node, old_child_num);
    cell[INTERNAL_NODE_CHILD_SIZE..INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE].copy_from_slice(&new_key.to_le_bytes());
}
pub fn print_node_contents(node: &mut [u8]) {
    println!("-------------------------------------------------");
    match get_node_type(node) {
        NodeType::NodeInternal => {
            println!("Node type: Internal");
            println!("Is root: {}", is_node_root(node));
            println!("Num keys: {}", internal_node_num_keys(node));
            println!("Parent: {}", get_node_parent(node));
            println!(
                "Right child: {}",
                usize::from_le_bytes(internal_node_right_child(node).try_into().unwrap())
            );
            for i in 0..internal_node_num_keys(node) {
                println!(
                    "Key: {}",
                    u32::from_le_bytes(internal_node_key(node, i as usize).try_into().unwrap())
                );
                println!(
                    "Child: {}",
                    usize::from_le_bytes(internal_node_child(node, i as usize).try_into().unwrap())
                );
            }
        }
        NodeType::NodeLeaf => {
            println!("Node type: Leaf");
            println!("Is root: {}", is_node_root(node));
            println!("Num cells: {}", leaf_node_num_cells(node));
            println!("Parent: {}", get_node_parent(node));
            for i in 0..leaf_node_num_cells(node) {
                print_cell(node, i as usize);
            }
        }
    }
    println!("-------------------------------------------------");
}

pub fn print_cell(node: &mut [u8], cell_num: usize) {
    println!("-------------------------------------------------");
    println!(
        "Key: {}",
        u32::from_le_bytes(leaf_node_key(node, cell_num as usize).try_into().unwrap())
    );
    let row = Row::deserialize_row(leaf_node_value(node, cell_num as usize));
    println!("Row: ({}, {}, {})", row.id, row.username, row.email);
    println!("-------------------------------------------------");
}
