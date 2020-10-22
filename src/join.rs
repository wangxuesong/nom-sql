use std::fmt;
use std::str;

use column::Column;
use condition::ConditionExpression;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::IResult;
use select::{JoinClause, SelectStatement};
use table::Table;
use Span;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinRightSide {
    /// A single table.
    Table(Table),
    /// A comma-separated (and implicitly joined) sequence of tables.
    Tables(Vec<Table>),
    /// A nested selection, represented as (query, alias).
    NestedSelect(Box<SelectStatement>, Option<String>),
    /// A nested join clause.
    NestedJoin(Box<JoinClause>),
}

impl fmt::Display for JoinRightSide {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JoinRightSide::Table(ref t) => write!(f, "{}", t)?,
            JoinRightSide::NestedSelect(ref q, ref a) => {
                write!(f, "({})", q)?;
                if a.is_some() {
                    write!(f, " AS {}", a.as_ref().unwrap())?;
                }
            }
            JoinRightSide::NestedJoin(ref jc) => write!(f, "({})", jc)?,
            _ => unimplemented!(),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinOperator {
    Join,
    LeftJoin,
    LeftOuterJoin,
    RightJoin,
    InnerJoin,
    CrossJoin,
    StraightJoin,
}

impl fmt::Display for JoinOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JoinOperator::Join => write!(f, "JOIN")?,
            JoinOperator::LeftJoin => write!(f, "LEFT JOIN")?,
            JoinOperator::LeftOuterJoin => write!(f, "LEFT OUTER JOIN")?,
            JoinOperator::RightJoin => write!(f, "RIGHT JOIN")?,
            JoinOperator::InnerJoin => write!(f, "INNER JOIN")?,
            JoinOperator::CrossJoin => write!(f, "CROSS JOIN")?,
            JoinOperator::StraightJoin => write!(f, "STRAIGHT JOIN")?,
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinConstraint {
    On(ConditionExpression),
    Using(Vec<Column>),
}

impl fmt::Display for JoinConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JoinConstraint::On(ref ce) => write!(f, "ON {}", ce)?,
            JoinConstraint::Using(ref columns) => write!(
                f,
                "USING ({})",
                columns
                    .iter()
                    .map(|c| format!("{}", c))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?,
        }
        Ok(())
    }
}

// Parse binary comparison operators
pub fn join_operator(i: Span) -> IResult<Span, JoinOperator> {
    alt((
        map(tag_no_case("join"), |_| JoinOperator::Join),
        map(tag_no_case("left join"), |_| JoinOperator::LeftJoin),
        map(tag_no_case("left outer join"), |_| {
            JoinOperator::LeftOuterJoin
        }),
        map(tag_no_case("right join"), |_| JoinOperator::RightJoin),
        map(tag_no_case("inner join"), |_| JoinOperator::InnerJoin),
        map(tag_no_case("cross join"), |_| JoinOperator::CrossJoin),
        map(tag_no_case("straight_join"), |_| JoinOperator::StraightJoin),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{FieldDefinitionExpression, Operator};
    use condition::ConditionBase::*;
    use condition::ConditionExpression::{self, *};
    use condition::ConditionTree;
    use select::{selection, JoinClause, SelectStatement};
    use Position;

    fn table_from_str(name: &str, pos: Position) -> Table {
        Table{
            pos,
            name: String::from(name),
            alias: None,
            schema: None
        }
    }

    #[test]
    fn inner_join() {
        let qstring = "SELECT tags.* FROM tags \
                       INNER JOIN taggings ON tags.id = taggings.tag_id";

        let res = selection(Span::new(qstring.as_bytes()));

        let mut c1 = Column::from("tags.id");
        c1.pos = Position::new(1, 48);
        let mut c2 = Column::from("taggings.tag_id");
        c2.pos = Position::new(1, 58);
        let ct = ConditionTree {
            left: Box::new(Base(Field(c1))),
            right: Box::new(Base(Field(c2))),
            operator: Operator::Equal,
        };
        let join_cond = ConditionExpression::ComparisonOp(ct);
        let expected_stmt = SelectStatement {
            pos: Position::new(1, 1),
            tables: vec![table_from_str("tags", Position::new(1, 20))],
            fields: vec![FieldDefinitionExpression::AllInTable("tags".into())],
            join: vec![JoinClause {
                operator: JoinOperator::InnerJoin,
                right: JoinRightSide::Table(table_from_str("taggings", Position::new(1, 36))),
                constraint: JoinConstraint::On(join_cond),
            }],
            ..Default::default()
        };

        let q = res.unwrap().1;
        assert_eq!(q, expected_stmt);
        assert_eq!(qstring, format!("{}", q));
    }
}
