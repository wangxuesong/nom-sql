extern crate nom;
extern crate nom_locate;

extern crate serde;
#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

pub use self::arithmetic::{ArithmeticBase, ArithmeticExpression, ArithmeticOperator};
pub use self::case::{CaseWhenExpression, ColumnOrLiteral};
pub use self::column::{
    Column, ColumnConstraint, ColumnSpecification, FunctionArgument, FunctionExpression,
};
pub use self::common::{
    FieldDefinitionExpression, FieldValueExpression, Literal, LiteralExpression, Operator, Real,
    SqlType, TableKey,
};
pub use self::compound_select::{CompoundSelectOperator, CompoundSelectStatement};
pub use self::condition::{ConditionBase, ConditionExpression, ConditionTree};
pub use self::create::{CreateTableStatement, CreateViewStatement, SelectSpecification};
pub use self::delete::DeleteStatement;
pub use self::insert::InsertStatement;
pub use self::join::{JoinConstraint, JoinOperator, JoinRightSide};
pub use self::order::{OrderClause, OrderType};
pub use self::parser::*;
pub use self::select::{GroupByClause, JoinClause, LimitClause, SelectStatement};
pub use self::set::SetStatement;
pub use self::table::Table;
pub use self::update::UpdateStatement;
use nom_locate::LocatedSpan;

pub type Span<'a> = LocatedSpan<&'a [u8]>;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub line: u32,
    pub offset: usize,
}

impl Position {
    pub fn new(line: u32, offset: usize) -> Position {
        Position { line, offset }
    }

    pub fn new_empty() -> Position {
        Position { line: 0, offset: 0 }
    }

    fn from_span(span: Span) -> Position {
        Position{ line: span.location_line(), offset: span.get_column() }
    }
}

impl<'a> From<Span<'a>> for Position {
    fn from(s: Span<'a>) -> Self {
        Position::from_span(s)
    }
}

pub mod parser;

#[macro_use]
mod keywords;
mod arithmetic;
mod case;
mod column;
mod common;
mod compound_select;
mod condition;
mod create;
mod create_table_options;
mod delete;
mod drop;
mod insert;
mod join;
mod order;
mod select;
mod set;
mod table;
mod update;
mod plsql;
