use crate::statement::Statement;

pub enum NodeType {
    NodeInternal,
    NodeLeaf,
}

pub enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableDuplicateKey,
    ExecuteTableFull,
}

pub enum MetaCommandResult {
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand,
}

pub enum PrepareResult {
    PrepareSuccess(Statement),
    PrepareUnrecognizedStatement,
    PrepareSyntaxError,
    PrepareNegativeId,
}