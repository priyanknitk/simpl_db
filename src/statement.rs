use crate::row::Row;

pub struct Statement {
    pub statement_type: StatementType,
    pub row_to_insert: Option<Row>,
}

pub enum StatementType {
    StatementInsert,
    StatementSelect,
}