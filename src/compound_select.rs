use nom::character::complete::{multispace0, multispace1};
use std::fmt;
use std::str;

use common::{opt_delimited, statement_terminator};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::many1;
use nom::sequence::{delimited, preceded, tuple};
use nom::IResult;
use order::{order_clause, OrderClause};
use select::{limit_clause, nested_selection, LimitClause, SelectStatement};
use {Position, Span};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub enum CompoundSelectOperator {
    Union,
    DistinctUnion,
    Intersect,
    Except,
}

impl fmt::Display for CompoundSelectOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CompoundSelectOperator::Union => write!(f, "UNION"),
            CompoundSelectOperator::DistinctUnion => write!(f, "UNION DISTINCT"),
            CompoundSelectOperator::Intersect => write!(f, "INTERSECT"),
            CompoundSelectOperator::Except => write!(f, "EXCEPT"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct CompoundSelectStatement {
    pub pos: Position,
    pub selects: Vec<(Option<CompoundSelectOperator>, SelectStatement)>,
    pub order: Option<OrderClause>,
    pub limit: Option<LimitClause>,
}

impl fmt::Display for CompoundSelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (ref op, ref sel) in &self.selects {
            if op.is_some() {
                write!(f, " {}", op.as_ref().unwrap())?;
            }
            write!(f, " {}", sel)?;
        }
        if self.order.is_some() {
            write!(f, " {}", self.order.as_ref().unwrap())?;
        }
        if self.limit.is_some() {
            write!(f, " {}", self.order.as_ref().unwrap())?;
        }
        Ok(())
    }
}

// Parse compound operator
fn compound_op(i: Span) -> IResult<Span, CompoundSelectOperator> {
    alt((
        map(
            preceded(
                tag_no_case("union"),
                opt(preceded(
                    multispace1,
                    alt((
                        map(tag_no_case("all"), |_| false),
                        map(tag_no_case("distinct"), |_| true),
                    )),
                )),
            ),
            |distinct| match distinct {
                // DISTINCT is the default in both MySQL and SQLite
                None => CompoundSelectOperator::DistinctUnion,
                Some(d) => {
                    if d {
                        CompoundSelectOperator::DistinctUnion
                    } else {
                        CompoundSelectOperator::Union
                    }
                }
            },
        ),
        map(tag_no_case("intersect"), |_| {
            CompoundSelectOperator::Intersect
        }),
        map(tag_no_case("except"), |_| CompoundSelectOperator::Except),
    ))(i)
}

fn other_selects(i: Span) -> IResult<Span, (Option<CompoundSelectOperator>, SelectStatement)> {
    let (remaining_input, (_, op, _, select)) = tuple((
        multispace0,
        compound_op,
        multispace1,
        opt_delimited(
            tag("("),
            delimited(multispace0, nested_selection, multispace0),
            tag(")"),
        ),
    ))(i)?;

    Ok((remaining_input, (Some(op), select)))
}

// Parse compound selection
pub fn compound_selection(i: Span) -> IResult<Span, CompoundSelectStatement> {
    let (remaining_input, (first_select, other_selects, _, order, limit, _)) = tuple((
        opt_delimited(tag("("), nested_selection, tag(")")),
        many1(other_selects),
        multispace0,
        opt(order_clause),
        opt(limit_clause),
        statement_terminator,
    ))(i)?;

    let mut selects = vec![(None, first_select)];
    selects.extend(other_selects);

    Ok((
        remaining_input,
        CompoundSelectStatement {
            pos: Position::from(i),
            selects,
            order,
            limit,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use common::{FieldDefinitionExpression, FieldValueExpression, Literal};
    use table::Table;
    use Position;

    fn table_from_str(name: &str, pos: Position) -> Table {
        Table {
            pos,
            name: String::from(name),
            alias: None,
            schema: None,
        }
    }

    #[test]
    fn union() {
        let qstr = "SELECT id, 1 FROM Vote UNION SELECT id, stars from Rating;";
        let qstr2 = "(SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating);";
        let res = compound_selection(Span::new(qstr.as_bytes()));
        let res2 = compound_selection(Span::new(qstr2.as_bytes()));

        let mut c1 = Column::from("id");
        c1.pos = Position::new(1, 8);
        let first_select = SelectStatement {
            pos: Position::new(1, 1),
            tables: vec![table_from_str("Vote", Position::new(1, 19))],
            fields: vec![
                FieldDefinitionExpression::Col(c1),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(1).into(),
                )),
            ],
            ..Default::default()
        };
        let mut c2 = Column::from("id");
        c2.pos = Position::new(1, 37);
        let mut c3 = Column::from("stars");
        c3.pos = Position::new(1, 41);
        let second_select = SelectStatement {
            pos: Position::new(1, 30),
            tables: vec![table_from_str("Rating", Position::new(1, 52))],
            fields: vec![
                FieldDefinitionExpression::Col(c2),
                FieldDefinitionExpression::Col(c3),
            ],
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            pos: Position::new(1, 1),
            selects: vec![
                (None, first_select.clone()),
                (
                    Some(CompoundSelectOperator::DistinctUnion),
                    second_select.clone(),
                ),
            ],
            order: None,
            limit: None,
        };

        let mut c1 = Column::from("id");
        c1.pos = Position::new(1, 9);
        let first_select2 = SelectStatement {
            pos: Position::new(1, 2),
            tables: vec![table_from_str("Vote", Position::new(1, 20))],
            fields: vec![
                FieldDefinitionExpression::Col(c1),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(1).into(),
                )),
            ],
            ..Default::default()
        };
        let mut c2 = Column::from("id");
        c2.pos = Position::new(1, 40);
        let mut c3 = Column::from("stars");
        c3.pos = Position::new(1, 44);
        let second_select2 = SelectStatement {
            pos: Position::new(1, 33),
            tables: vec![table_from_str("Rating", Position::new(1, 55))],
            fields: vec![
                FieldDefinitionExpression::Col(c2),
                FieldDefinitionExpression::Col(c3),
            ],
            ..Default::default()
        };
        let expected2 = CompoundSelectStatement {
            pos: Position::new(1, 1),
            selects: vec![
                (None, first_select2),
                (Some(CompoundSelectOperator::DistinctUnion), second_select2),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
        assert_eq!(res2.unwrap().1, expected2);
    }

    #[test]
    fn union_strict() {
        let qstr = "SELECT id, 1 FROM Vote);";
        let qstr2 = "(SELECT id, 1 FROM Vote;";
        let qstr3 = "SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating;";
        let res = compound_selection(Span::new(qstr.as_bytes()));
        let res2 = compound_selection(Span::new(qstr2.as_bytes()));
        let res3 = compound_selection(Span::new(qstr3.as_bytes()));

        assert!(&res.is_err());
        assert_eq!(
            res.unwrap_err(),
            nom::Err::Error(nom::error::Error::new(
                unsafe { Span::new_from_raw_offset(22, 1, ");".as_bytes(), ()) },
                nom::error::ErrorKind::Tag
            ))
        );
        assert!(&res2.is_err());
        assert_eq!(
            res2.unwrap_err(),
            nom::Err::Error(nom::error::Error::new(
                unsafe { Span::new_from_raw_offset(23, 1, ";".as_bytes(), ()) },
                nom::error::ErrorKind::Tag
            ))
        );
        assert!(&res3.is_err());
        assert_eq!(
            res3.unwrap_err(),
            nom::Err::Error(nom::error::Error::new(
                unsafe {
                    Span::new_from_raw_offset(
                        22,
                        1,
                        ") UNION (SELECT id, stars from Rating;".as_bytes(),
                        (),
                    )
                },
                nom::error::ErrorKind::Tag
            ))
        );
    }

    #[test]
    fn multi_union() {
        let qstr = "SELECT id, 1 FROM Vote \
                    UNION SELECT id, stars from Rating \
                    UNION DISTINCT SELECT 42, 5 FROM Vote;";
        let res = compound_selection(Span::new(qstr.as_bytes()));

        let mut column1 = Column::from("id");
        column1.pos = Position::new(1, 8);
        let first_select = SelectStatement {
            pos: Position::new(1, 1),
            tables: vec![table_from_str("Vote", Position::new(1, 19))],
            fields: vec![
                FieldDefinitionExpression::Col(column1),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(1).into(),
                )),
            ],
            ..Default::default()
        };
        let mut column2 = Column::from("id");
        column2.pos = Position::new(1, 37);
        let mut column3 = Column::from("stars");
        column3.pos = Position::new(1, 41);
        let second_select = SelectStatement {
            pos: Position::new(1, 30),
            tables: vec![table_from_str("Rating", Position::new(1, 52))],
            fields: vec![
                FieldDefinitionExpression::Col(column2),
                FieldDefinitionExpression::Col(column3),
            ],
            ..Default::default()
        };
        let third_select = SelectStatement {
            pos: Position::new(1, 74),
            tables: vec![table_from_str("Vote", Position::new(1, 92))],
            fields: vec![
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(42).into(),
                )),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(5).into(),
                )),
            ],
            ..Default::default()
        };

        let expected = CompoundSelectStatement {
            pos: Position::new(1, 1),
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::DistinctUnion), second_select),
                (Some(CompoundSelectOperator::DistinctUnion), third_select),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn union_all() {
        let qstr = "SELECT id, 1 FROM Vote UNION ALL SELECT id, stars from Rating;";
        let res = compound_selection(Span::new(qstr.as_bytes()));

        let mut c1 = Column::from("id");
        c1.pos = Position::new(1, 8);
        let first_select = SelectStatement {
            pos: Position::new(1, 1),
            tables: vec![table_from_str("Vote", Position::new(1, 19))],
            fields: vec![
                FieldDefinitionExpression::Col(c1),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(1).into(),
                )),
            ],
            ..Default::default()
        };
        let mut c2 = Column::from("id");
        c2.pos = Position::new(1, 41);
        let mut c3 = Column::from("stars");
        c3.pos = Position::new(1, 45);
        let second_select = SelectStatement {
            pos: Position::new(1, 34),
            tables: vec![table_from_str("Rating", Position::new(1, 56))],
            fields: vec![
                FieldDefinitionExpression::Col(c2),
                FieldDefinitionExpression::Col(c3),
            ],
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            pos: Position::new(1, 1),
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::Union), second_select),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
    }
}
