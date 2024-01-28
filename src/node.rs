use crate::constants::*;
use crate::cursor::Cursor;
use crate::enums::NodeType;
use crate::row::Row;
use crate::table::Table;

pub fn leaf_node_num_cells(node: &[u8]) -> u32 {
    let num_cells_slice =
        &node[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE];
    u32::from_le_bytes(num_cells_slice.try_into().unwrap())
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
    node[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
        .copy_from_slice(&[0; LEAF_NODE_NUM_CELLS_SIZE]);
    set_node_type(node, NodeType::NodeLeaf);
    set_node_root(node, false);
}

pub fn initialize_internal_node(node: &mut [u8]) {
    set_node_type(node, NodeType::NodeInternal);
    set_node_root(node, false);
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
    node[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
        .copy_from_slice(&(num_cells + 1).to_le_bytes());
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
    let new_node_buffer = &mut [b'0'; PAGE_SIZE][..];
    let old_node = cursor.table.pager.get_page(cursor.page_num);

    // copy old node to a temporary buffer
    let old_node_buffer = &mut [b'0'; PAGE_SIZE];
    old_node_buffer.copy_from_slice(old_node);

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

    let new_node = cursor.table.pager.get_page(new_page_num);

    // copy new_node_buffer to new_node
    new_node.copy_from_slice(new_node_buffer);

    initialize_leaf_node(new_node);

    // Update cell count on both leaf nodes
    let old_node_num_cells = LEAF_NODE_LEFT_SPLIT_COUNT as u32;
    let new_node_num_cells = LEAF_NODE_RIGHT_SPLIT_COUNT as u32;

    new_node[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
        .copy_from_slice(&new_node_num_cells.to_le_bytes());

    let old_node = cursor.table.pager.get_page(cursor.page_num);

    // copy old_node_buffer to old_node
    old_node.copy_from_slice(old_node_buffer);

    old_node[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
        .copy_from_slice(&old_node_num_cells.to_le_bytes());
    if is_node_root(old_node) {
        return create_new_root(cursor.table, new_page_num);
    } else {
        panic!("Need to implement updating parent after split.");
    }
}

pub fn is_node_root(node: &[u8]) -> bool {
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

pub fn create_new_root(table: &mut Table, right_child_page_num: usize) {
    let left_child_page_num = table.pager.get_unused_page_num();

    let root = table.pager.get_page(table.root_page_num);
    // create a buffer to hold the old root node
    let new_root_buffer = &mut [b'0'; PAGE_SIZE];
    new_root_buffer.copy_from_slice(root);

    let new_left_child_buffer = &mut [b'0'; PAGE_SIZE];

    new_left_child_buffer.copy_from_slice(new_root_buffer);
    set_node_root(new_left_child_buffer, false);

    initialize_internal_node(new_root_buffer);
    set_node_root(new_root_buffer, true);
    let num_keys_root_value:u32 = 1;
    internal_node_num_keys_mut(new_root_buffer).copy_from_slice(&num_keys_root_value.to_le_bytes());
    internal_node_child(new_root_buffer, 0).copy_from_slice(&left_child_page_num.to_le_bytes());
    let left_child_max_key = get_node_max_key(new_left_child_buffer);
    internal_node_key_mut(new_root_buffer, 0).copy_from_slice(&left_child_max_key.to_le_bytes());
    internal_node_right_child(new_root_buffer).copy_from_slice(&right_child_page_num.to_le_bytes());

    // copy the buffers back to the pages
    let root = table.pager.get_page(table.root_page_num);
    root.copy_from_slice(new_root_buffer);
    let left_child = table.pager.get_page(left_child_page_num);
    left_child.copy_from_slice(new_left_child_buffer);
}

pub fn internal_node_num_keys(node: &[u8]) -> u32 {
    let num_keys_slice = &node[INTERNAL_NODE_NUM_KEYS_OFFSET
        ..INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE];
    u32::from_le_bytes(num_keys_slice.try_into().unwrap())
}

pub fn internal_node_num_keys_mut(node: &mut [u8]) -> &mut [u8] {
    node[INTERNAL_NODE_NUM_KEYS_OFFSET..INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE]
        .as_mut()
}

pub fn internal_node_right_child(node: &mut [u8]) -> &mut [u8] {
    node[INTERNAL_NODE_RIGHT_CHILD_OFFSET
        ..INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE]
        .as_mut()
}

pub fn internal_node_cell(node: &[u8], cell_num: usize) -> &[u8] {
    let cell_offset = INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
    &node[cell_offset..cell_offset + INTERNAL_NODE_CELL_SIZE]
}

pub fn internal_node_cell_mut(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    let cell_offset = INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
    &mut node[cell_offset..cell_offset + INTERNAL_NODE_CELL_SIZE]
}

pub fn internal_node_key(node: &[u8], cell_num: usize) -> &[u8] {
    let cell = internal_node_cell(node, cell_num);
    &cell[0..INTERNAL_NODE_KEY_SIZE]
}

pub fn internal_node_key_mut(node: &mut [u8], cell_num: usize) -> &mut [u8] {
    let cell = internal_node_cell_mut(node, cell_num);
    &mut cell[..INTERNAL_NODE_KEY_SIZE]
}

pub fn internal_node_child(node: &mut [u8], child_num: usize) -> &mut [u8] {
    let num_keys = internal_node_num_keys(node);
    if child_num as u32 > num_keys {
        eprintln!("Tried to access child {} > {}.", child_num, num_keys);
        panic!("Tried to access child out of bounds.");
    } else if child_num as u32 == num_keys {
        internal_node_right_child(node)
    } else {
        internal_node_cell_mut(node, child_num)
    }
}

pub fn get_node_max_key(node: &[u8]) -> u32 {
    match get_node_type(node) {
        NodeType::NodeInternal => {
            let num_keys = internal_node_num_keys(node);
            let max_key = u32::from_le_bytes(
                internal_node_key(node, num_keys as usize - 1)
                    .try_into()
                    .unwrap(),
            );
            max_key
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

pub fn print_node_contents(node: &mut [u8]) {
    println!("-------------------------------------------------");
    match get_node_type(node) {
        NodeType::NodeInternal => {
            println!("Node type: Internal");
            println!("Is root: {}", is_node_root(node));
            println!("Num keys: {}", internal_node_num_keys(node));
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