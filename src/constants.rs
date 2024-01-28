use std::mem;

pub const ID_SIZE: usize = mem::size_of::<i32>();
pub const USERNAME_SIZE: usize = mem::size_of::<String>();
pub const EMAIL_SIZE: usize = mem::size_of::<String>();

pub const ID_OFFSET: usize = 0;
pub const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
pub const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;

pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub const PAGE_SIZE: usize = 128;
pub const TABLE_MAX_PAGES: usize = 100;

pub const NODE_TYPE_OFFSET: usize = 0;
pub const NODE_TYPE_SIZE: usize = mem::size_of::<u8>();

pub const IS_ROOT_SIZE: usize = mem::size_of::<u8>();
pub const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;

pub const PARENT_POINTER_SIZE: usize = mem::size_of::<u32>();

pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

pub const LEAF_NODE_NUM_CELLS_SIZE: usize = mem::size_of::<u32>();
pub const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;

pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

pub const LEAF_NODE_KEY_SIZE: usize = mem::size_of::<u32>();
pub const LEAF_NODE_KEY_OFFSET: usize = 0;

pub const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;

pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;

pub const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;

pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

pub const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
pub const LEAF_NODE_LEFT_SPLIT_COUNT: usize = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_RIGHT_SPLIT_COUNT;

// Internal Node Header Layout
pub const INTERNAL_NODE_NUM_KEYS_SIZE: usize = mem::size_of::<u32>();
pub const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = mem::size_of::<usize>();
pub const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
pub const INTERNAL_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// Internal Node Body Layout
pub const INTERNAL_NODE_KEY_SIZE: usize = mem::size_of::<u32>();
pub const INTERNAL_NODE_CHILD_SIZE: usize = mem::size_of::<u32>();
pub const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
