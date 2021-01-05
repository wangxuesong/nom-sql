use nom::character::complete::{multispace0, multispace1};
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::str;

use arithmetic::{arithmetic_expression, ArithmeticExpression};
use column::Column;
use common::{
    binary_comparison_operator, column_identifier, literal, value_list, Literal, Operator,
};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use nom::IResult;
use select::{nested_selection, SelectStatement};
use Span;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ConditionBase {
    Field(Column),
    Literal(Literal),
    LiteralList(Vec<Literal>),
    NestedSelect(Box<SelectStatement>),
}

impl fmt::Display for ConditionBase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConditionBase::Field(ref col) => write!(f, "{}", col),
            ConditionBase::Literal(ref literal) => write!(f, "{}", literal.to_string()),
            ConditionBase::LiteralList(ref ll) => write!(
                f,
                "({})",
                ll.iter()
                    .map(|l| l.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ConditionBase::NestedSelect(ref select) => write!(f, "{}", select),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ConditionTree {
    pub operator: Operator,
    pub left: Box<ConditionExpression>,
    pub right: Box<ConditionExpression>,
}

impl<'a> ConditionTree {
    pub fn contained_columns(&'a self) -> HashSet<&'a Column> {
        let mut s = HashSet::new();
        let mut q = VecDeque::<&'a ConditionTree>::new();
        q.push_back(self);
        while let Some(ref ct) = q.pop_front() {
            match *ct.left.as_ref() {
                ConditionExpression::Base(ConditionBase::Field(ref c)) => {
                    s.insert(c);
                }
                ConditionExpression::LogicalOp(ref ct)
                | ConditionExpression::ComparisonOp(ref ct) => q.push_back(ct),
                _ => (),
            }
            match *ct.right.as_ref() {
                ConditionExpression::Base(ConditionBase::Field(ref c)) => {
                    s.insert(c);
                }
                ConditionExpression::LogicalOp(ref ct)
                | ConditionExpression::ComparisonOp(ref ct) => q.push_back(ct),
                _ => (),
            }
        }
        s
    }
}

impl fmt::Display for ConditionTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.left)?;
        write!(f, " {} ", self.operator)?;
        write!(f, "{}", self.right)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ConditionExpression {
    ComparisonOp(ConditionTree),
    LogicalOp(ConditionTree),
    NegationOp(Box<ConditionExpression>),
    ExistsOp(Box<SelectStatement>),
    Base(ConditionBase),
    Arithmetic(Box<ArithmeticExpression>),
    Bracketed(Box<ConditionExpression>),
}

impl fmt::Display for ConditionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConditionExpression::ComparisonOp(ref tree) => write!(f, "{}", tree),
            ConditionExpression::LogicalOp(ref tree) => write!(f, "{}", tree),
            ConditionExpression::NegationOp(ref expr) => write!(f, "NOT {}", expr),
            ConditionExpression::ExistsOp(ref expr) => write!(f, "EXISTS {}", expr),
            ConditionExpression::Bracketed(ref expr) => write!(f, "({})", expr),
            ConditionExpression::Base(ref base) => write!(f, "{}", base),
            ConditionExpression::Arithmetic(ref expr) => write!(f, "{}", expr),
        }
    }
}

// Parse a conditional expression into a condition tree structure
pub fn condition_expr(i: Span) -> IResult<Span, ConditionExpression> {
    let cond = map(
        separated_pair(
            and_expr,
            delimited(multispace0, tag_no_case("or"), multispace1),
            condition_expr,
        ),
        |p| {
            ConditionExpression::LogicalOp(ConditionTree {
                operator: Operator::Or,
                left: Box::new(p.0),
                right: Box::new(p.1),
            })
        },
    );

    alt((cond, and_expr))(i)
}

pub fn and_expr(i: Span) -> IResult<Span, ConditionExpression> {
    let cond = map(
        separated_pair(
            parenthetical_expr,
            delimited(multispace0, tag_no_case("and"), multispace1),
            and_expr,
        ),
        |p| {
            ConditionExpression::LogicalOp(ConditionTree {
                operator: Operator::And,
                left: Box::new(p.0),
                right: Box::new(p.1),
            })
        },
    );

    alt((cond, parenthetical_expr))(i)
}

fn parenthetical_expr_helper(i: Span) -> IResult<Span, ConditionExpression> {
    let (remaining_input, (_, _, left_expr, _, _, _, operator, _, right_expr)) = tuple((
        tag("("),
        multispace0,
        simple_expr,
        multispace0,
        tag(")"),
        multispace0,
        binary_comparison_operator,
        multispace0,
        simple_expr,
    ))(i)?;

    let left = Box::new(ConditionExpression::Bracketed(Box::new(left_expr)));
    let right = Box::new(right_expr);
    let cond = ConditionExpression::ComparisonOp(ConditionTree {
        operator,
        left,
        right,
    });

    Ok((remaining_input, cond))
}

pub fn parenthetical_expr(i: Span) -> IResult<Span, ConditionExpression> {
    alt((
        parenthetical_expr_helper,
        map(
            delimited(
                terminated(tag("("), multispace0),
                condition_expr,
                delimited(multispace0, tag(")"), multispace0),
            ),
            |inner| ConditionExpression::Bracketed(Box::new(inner)),
        ),
        not_expr,
    ))(i)
}

pub fn not_expr(i: Span) -> IResult<Span, ConditionExpression> {
    alt((
        map(
            preceded(pair(tag_no_case("not"), multispace1), parenthetical_expr),
            |right| ConditionExpression::NegationOp(Box::new(right)),
        ),
        boolean_primary,
    ))(i)
}

fn is_null(i: Span) -> IResult<Span, (Operator, ConditionExpression)> {
    let (remaining_input, (_, _, not, _, _)) = tuple((
        tag_no_case("is"),
        multispace0,
        opt(tag_no_case("not")),
        multispace0,
        tag_no_case("null"),
    ))(i)?;

    // XXX(malte): bit of a hack; would consumers ever need to know
    // about "IS NULL" vs. "= NULL"?
    Ok((
        remaining_input,
        (
            if not.is_some() {
                Operator::NotEqual
            } else {
                Operator::Equal
            },
            ConditionExpression::Base(ConditionBase::Literal(Literal::Null)),
        ),
    ))
}

fn in_operation(i: Span) -> IResult<Span, (Operator, ConditionExpression)> {
    map(
        separated_pair(
            opt(terminated(tag_no_case("not"), multispace1)),
            terminated(tag_no_case("in"), multispace0),
            alt((
                map(delimited(tag("("), nested_selection, tag(")")), |s| {
                    ConditionBase::NestedSelect(Box::new(s))
                }),
                map(delimited(tag("("), value_list, tag(")")), |vs| {
                    ConditionBase::LiteralList(vs)
                }),
            )),
        ),
        |p| {
            let nested = ConditionExpression::Base(p.1);
            if (p.0).is_some() {
                (Operator::NotIn, nested)
            } else {
                (Operator::In, nested)
            }
        },
    )(i)
}

fn boolean_primary_rest(i: Span) -> IResult<Span, (Operator, ConditionExpression)> {
    alt((
        is_null,
        in_operation,
        separated_pair(binary_comparison_operator, multispace0, predicate),
    ))(i)
}

fn boolean_primary(i: Span) -> IResult<Span, ConditionExpression> {
    alt((
        map(
            separated_pair(predicate, multispace0, boolean_primary_rest),
            |e: (ConditionExpression, (Operator, ConditionExpression))| {
                ConditionExpression::ComparisonOp(ConditionTree {
                    operator: (e.1).0,
                    left: Box::new(e.0),
                    right: Box::new((e.1).1),
                })
            },
        ),
        predicate,
    ))(i)
}

fn predicate(i: Span) -> IResult<Span, ConditionExpression> {
    let nested_exists = map(
        tuple((
            opt(delimited(multispace0, tag_no_case("not"), multispace1)),
            delimited(multispace0, tag_no_case("exists"), multispace0),
            delimited(
                pair(tag("("), multispace0),
                nested_selection,
                pair(multispace0, tag(")")),
            ),
        )),
        |p| {
            let nested = ConditionExpression::ExistsOp(Box::new(p.2));
            if (p.0).is_some() {
                ConditionExpression::NegationOp(Box::new(nested))
            } else {
                nested
            }
        },
    );

    alt((simple_expr, nested_exists))(i)
}

fn simple_expr(i: Span) -> IResult<Span, ConditionExpression> {
    alt((
        map(
            delimited(
                terminated(tag("("), multispace0),
                arithmetic_expression,
                preceded(multispace0, tag(")")),
            ),
            |e| {
                ConditionExpression::Bracketed(Box::new(ConditionExpression::Arithmetic(Box::new(
                    e,
                ))))
            },
        ),
        map(arithmetic_expression, |e| {
            ConditionExpression::Arithmetic(Box::new(e))
        }),
        map(literal, |lit| {
            ConditionExpression::Base(ConditionBase::Literal(lit))
        }),
        map(column_identifier, |f| {
            ConditionExpression::Base(ConditionBase::Field(f))
        }),
        map(delimited(tag("("), nested_selection, tag(")")), |s| {
            ConditionExpression::Base(ConditionBase::NestedSelect(Box::new(s)))
        }),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arithmetic::{ArithmeticBase, ArithmeticOperator};
    use column::Column;
    use common::{FieldDefinitionExpression, ItemPlaceholder, Literal, Operator};
    use Position;

    fn columns(cols: &[(&str, Position)]) -> Vec<FieldDefinitionExpression> {
        cols.iter()
            .map(|c| FieldDefinitionExpression::Col(column(c)))
            .collect()
    }

    fn column(c: &(&str, Position)) -> Column {
        let mut col = Column::from((*c).0);
        col.pos = (*c).1.clone();
        col
    }

    fn flat_condition_tree(
        op: Operator,
        l: ConditionBase,
        r: ConditionBase,
    ) -> ConditionExpression {
        ConditionExpression::ComparisonOp(ConditionTree {
            operator: op,
            left: Box::new(ConditionExpression::Base(l)),
            right: Box::new(ConditionExpression::Base(r)),
        })
    }

    #[test]
    fn ct_contained_columns() {
        use std::collections::HashSet;

        let cond = "a.foo = ? and b.bar = 42";

        let res = condition_expr(Span::new(cond.as_bytes()));
        let mut c1 = Column::from("a.foo");
        c1.pos = Position::new(1, 1);
        let mut c2 = Column::from("b.bar");
        c2.pos = Position::new(1, 15);
        let mut expected_cols = HashSet::new();
        expected_cols.insert(&c1);
        expected_cols.insert(&c2);
        match res.unwrap().1 {
            ConditionExpression::LogicalOp(ct) => {
                assert_eq!(ct.contained_columns(), expected_cols);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn equality_placeholder() {
        x_equality_variable_placeholder(
            "foo = ?",
            Literal::Placeholder(ItemPlaceholder::QuestionMark),
            Position::new(1, 1),
        );
    }

    #[test]
    fn equality_variable_placeholder() {
        x_equality_variable_placeholder(
            "foo = :12",
            Literal::Placeholder(ItemPlaceholder::ColonNumber(12)),
            Position::new(1, 1),
        );
    }

    #[test]
    fn equality_variable_placeholder_with_dollar_sign() {
        x_equality_variable_placeholder(
            "foo = $12",
            Literal::Placeholder(ItemPlaceholder::DollarNumber(12)),
            Position::new(1, 1),
        );
    }

    fn x_equality_variable_placeholder(cond: &str, literal: Literal, pos: Position) {
        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(column(&("foo", pos))),
                ConditionBase::Literal(literal)
            )
        );
    }

    fn x_operator_value(
        op: ArithmeticOperator,
        value: Literal,
        pos: Position,
    ) -> ConditionExpression {
        let mut column = Column::from("x");
        column.pos = pos;
        ConditionExpression::Arithmetic(Box::new(ArithmeticExpression::new(
            op,
            ArithmeticBase::Column(column),
            ArithmeticBase::Scalar(value),
            None,
        )))
    }
    #[test]
    fn simple_arithmetic_expression() {
        let cond = "x + 3";

        let res = simple_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            x_operator_value(ArithmeticOperator::Add, 3.into(), Position::new(1, 1))
        );
    }

    #[test]
    fn simple_arithmetic_expression_with_parenthesis() {
        let cond = "( x - 2 )";

        let res = simple_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(x_operator_value(
                ArithmeticOperator::Subtract,
                2.into(),
                Position::new(1, 3)
            )))
        );
    }

    #[test]
    fn parenthetical_arithmetic_expression() {
        let cond = "( x * 5 )";

        let res = parenthetical_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(x_operator_value(
                ArithmeticOperator::Multiply,
                5.into(),
                Position::new(1, 3)
            )))
        );
    }

    #[test]
    fn condition_expression_with_arithmetics() {
        let cond = "x * 3 = 21";

        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: Box::new(x_operator_value(
                    ArithmeticOperator::Multiply,
                    3.into(),
                    Position::new(1, 1)
                )),
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(21.into())))
            })
        );
    }
    #[test]
    fn condition_expression_with_arithmetics_and_parenthesis() {
        let cond = "(x - 7 = 15)";

        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(ConditionExpression::ComparisonOp(
                ConditionTree {
                    operator: Operator::Equal,
                    left: Box::new(x_operator_value(
                        ArithmeticOperator::Subtract,
                        7.into(),
                        Position::new(1, 2)
                    )),
                    right: Box::new(ConditionExpression::Base(ConditionBase::Literal(15.into())))
                }
            )))
        );
    }

    #[test]
    fn condition_expression_with_arithmetics_in_parenthesis() {
        let cond = "( x + 2) = 15";

        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: Box::new(ConditionExpression::Bracketed(Box::new(x_operator_value(
                    ArithmeticOperator::Add,
                    2.into(),
                    Position::new(1, 3)
                )))),
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(15.into())))
            })
        );
    }

    #[test]
    fn condition_expression_with_arithmetics_in_parenthesis_in_both_side() {
        let cond = "( x + 2) =(x*3)";

        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: Box::new(ConditionExpression::Bracketed(Box::new(x_operator_value(
                    ArithmeticOperator::Add,
                    2.into(),
                    Position::new(1, 3)
                )))),
                right: Box::new(ConditionExpression::Bracketed(Box::new(x_operator_value(
                    ArithmeticOperator::Multiply,
                    3.into(),
                    Position::new(1, 12)
                ))))
            })
        );
    }

    #[test]
    fn equality_literals() {
        let cond1 = "foo = 42";
        let cond2 = "foo = \"hello\"";

        let res1 = condition_expr(Span::new(cond1.as_bytes()));
        assert_eq!(
            res1.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(column(&("foo", Position::new(1, 1)))),
                ConditionBase::Literal(Literal::Integer(42 as i64))
            )
        );

        let res2 = condition_expr(Span::new(cond2.as_bytes()));
        assert_eq!(
            res2.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(column(&("foo", Position::new(1, 1)))),
                ConditionBase::Literal(Literal::String(String::from("hello")))
            )
        );
    }

    #[test]
    fn inequality_literals() {
        let cond1 = "foo >= 42";
        let cond2 = "foo <= 5";

        let res1 = condition_expr(Span::new(cond1.as_bytes()));
        assert_eq!(
            res1.unwrap().1,
            flat_condition_tree(
                Operator::GreaterOrEqual,
                ConditionBase::Field(column(&("foo", Position::new(1, 1)))),
                ConditionBase::Literal(Literal::Integer(42 as i64))
            )
        );

        let res2 = condition_expr(Span::new(cond2.as_bytes()));
        assert_eq!(
            res2.unwrap().1,
            flat_condition_tree(
                Operator::LessOrEqual,
                ConditionBase::Field(column(&("foo", Position::new(1, 1)))),
                ConditionBase::Literal(Literal::Integer(5 as i64))
            )
        );
    }

    #[test]
    fn empty_string_literal() {
        let cond = "foo = ''";

        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(column(&("foo", Position::new(1, 1)))),
                ConditionBase::Literal(Literal::String(String::from("")))
            )
        );
    }

    #[test]
    fn parenthesis() {
        let cond = "(foo = ? or bar = 12) and foobar = 'a'";

        use common::Literal;
        use ConditionBase::*;
        use ConditionExpression::*;

        let a = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(column(&("foo", Position::new(1, 2)))))),
            right: Box::new(Base(Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            )))),
        });

        let b = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(column(&("bar", Position::new(1, 13)))))),
            right: Box::new(Base(Literal(Literal::Integer(12.into())))),
        });

        let left = Bracketed(Box::new(LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Box::new(a),
            right: Box::new(b),
        })));

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(column(&("foobar", Position::new(1, 27)))))),
            right: Box::new(Base(Literal(Literal::String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::And,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn order_of_operations() {
        let cond = "foo = ? and bar = 12 or foobar = 'a'";

        use common::Literal;
        use ConditionBase::*;
        use ConditionExpression::*;

        let a = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(column(&("foo", Position::new(1, 1)))))),
            right: Box::new(Base(Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            )))),
        });

        let b = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(column(&("bar", Position::new(1, 13)))))),
            right: Box::new(Base(Literal(Literal::Integer(12.into())))),
        });

        let left = LogicalOp(ConditionTree {
            operator: Operator::And,
            left: Box::new(a),
            right: Box::new(b),
        });

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(column(&("foobar", Position::new(1, 25)))))),
            right: Box::new(Base(Literal(Literal::String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn negation() {
        let cond = "not bar = 12 or foobar = 'a'";

        use common::Literal::*;
        use ConditionBase::*;
        use ConditionExpression::*;

        let left = NegationOp(Box::new(ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(column(&("bar", Position::new(1, 5)))))),
            right: Box::new(Base(Literal(Integer(12.into())))),
        })));

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field(column(&("foobar", Position::new(1, 17)))))),
            right: Box::new(Base(Literal(String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(Span::new(cond.as_bytes()));
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn nested_select() {
        use select::SelectStatement;
        use std::default::Default;
        use table::Table;
        use ConditionBase::*;

        fn table_from_str(name: &str, pos: Position) -> Table {
            Table {
                pos,
                name: String::from(name),
                alias: None,
                schema: None,
            }
        }

        let cond = "bar in (select col from foo)";

        let res = condition_expr(Span::new(cond.as_bytes()));

        let nested_select = Box::new(SelectStatement {
            pos: Position::new(1, 9),
            tables: vec![table_from_str("foo", Position::new(1, 25))],
            fields: columns(&[("col", Position::new(1, 16))]),
            ..Default::default()
        });

        let expected = flat_condition_tree(
            Operator::In,
            Field(column(&("bar", Position::new(1, 1)))),
            NestedSelect(nested_select),
        );

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn exists_in_select() {
        use select::SelectStatement;
        use std::default::Default;
        use table::Table;

        fn table_from_str(name: &str, pos: Position) -> Table {
            Table {
                pos,
                name: String::from(name),
                alias: None,
                schema: None,
            }
        }

        let cond = "exists (  select col from foo  )";

        let res = condition_expr(Span::new(cond.as_bytes()));

        let nested_select = Box::new(SelectStatement {
            pos: Position::new(1, 11),
            tables: vec![table_from_str("foo", Position::new(1, 27))],
            fields: columns(&[("col", Position::new(1, 18))]),
            ..Default::default()
        });

        let expected = ConditionExpression::ExistsOp(nested_select);

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn not_exists_in_select() {
        use select::SelectStatement;
        use std::default::Default;
        use table::Table;

        fn table_from_str(name: &str, pos: Position) -> Table {
            Table {
                pos,
                name: String::from(name),
                alias: None,
                schema: None,
            }
        }

        let cond = "not exists (select col from foo)";

        let res = condition_expr(Span::new(cond.as_bytes()));

        let nested_select = Box::new(SelectStatement {
            pos: Position::new(1, 13),
            tables: vec![table_from_str("foo", Position::new(1, 29))],
            fields: columns(&[("col", Position::new(1, 20))]),
            ..Default::default()
        });

        let expected =
            ConditionExpression::NegationOp(Box::new(ConditionExpression::ExistsOp(nested_select)));

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn and_with_nested_select() {
        use select::SelectStatement;
        use std::default::Default;
        use table::Table;
        use ConditionBase::*;

        fn table_from_str(name: &str, pos: Position) -> Table {
            Table {
                pos,
                name: String::from(name),
                alias: None,
                schema: None,
            }
        }

        let cond = "paperId in (select paperId from PaperConflict) and size > 0";

        let res = condition_expr(Span::new(cond.as_bytes()));

        let nested_select = Box::new(SelectStatement {
            pos: Position::new(1, 13),
            tables: vec![table_from_str("PaperConflict", Position::new(1, 33))],
            fields: columns(&[("paperId", Position::new(1, 20))]),
            ..Default::default()
        });

        let left = flat_condition_tree(
            Operator::In,
            Field(column(&("paperId", Position::new(1, 1)))),
            NestedSelect(nested_select),
        );

        let right = flat_condition_tree(
            Operator::Greater,
            Field(column(&("size", Position::new(1, 52)))),
            Literal(0.into()),
        );

        let expected = ConditionExpression::LogicalOp(ConditionTree {
            left: Box::new(left),
            right: Box::new(right),
            operator: Operator::And,
        });

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn in_list_of_values() {
        use ConditionBase::*;

        let cond = "bar in (0)";

        let res = condition_expr(Span::new(cond.as_bytes()));

        let expected = flat_condition_tree(
            Operator::In,
            Field(column(&("bar", Position::new(1, 1)))),
            LiteralList(vec![0.into()]),
        );

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn is_null() {
        use common::Literal;
        use ConditionBase::*;

        let cond = "bar IS NULL";

        let res = condition_expr(Span::new(cond.as_bytes()));
        let expected = flat_condition_tree(
            Operator::Equal,
            Field(column(&("bar", Position::new(1, 1)))),
            Literal(Literal::Null),
        );
        assert_eq!(res.unwrap().1, expected);

        let cond = "bar IS NOT NULL";

        let res = condition_expr(Span::new(cond.as_bytes()));
        let expected = flat_condition_tree(
            Operator::NotEqual,
            Field(column(&("bar", Position::new(1, 1)))),
            Literal(Literal::Null),
        );
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn complex_bracketing() {
        use common::Literal;
        use ConditionBase::*;

        let cond = "`read_ribbons`.`is_following` = 1 \
                    AND `comments`.`user_id` <> `read_ribbons`.`user_id` \
                    AND `saldo` >= 0 \
                    AND ( `parent_comments`.`user_id` = `read_ribbons`.`user_id` \
                    OR ( `parent_comments`.`user_id` IS NULL \
                    AND `stories`.`user_id` = `read_ribbons`.`user_id` ) ) \
                    AND ( `parent_comments`.`id` IS NULL \
                    OR `saldo` >= 0 ) \
                    AND `read_ribbons`.`user_id` = ?";

        let res = condition_expr(Span::new(cond.as_bytes()));
        let expected = ConditionExpression::LogicalOp(ConditionTree {
            operator: Operator::And,
            left: Box::new(flat_condition_tree(
                Operator::Equal,
                Field(column(&("read_ribbons.is_following", Position::new(1, 1)))),
                Literal(Literal::Integer(1.into())),
            )),
            right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                operator: Operator::And,
                left: Box::new(flat_condition_tree(
                    Operator::NotEqual,
                    Field(column(&("comments.user_id", Position::new(1, 39)))),
                    Field(column(&("read_ribbons.user_id", Position::new(1, 63)))),
                )),
                right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                    operator: Operator::And,
                    left: Box::new(flat_condition_tree(
                        Operator::GreaterOrEqual,
                        Field(column(&("saldo", Position::new(1, 92)))),
                        Literal(Literal::Integer(0.into())),
                    )),
                    right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                        operator: Operator::And,
                        left: Box::new(ConditionExpression::Bracketed(Box::new(
                            ConditionExpression::LogicalOp(ConditionTree {
                                operator: Operator::Or,
                                left: Box::new(flat_condition_tree(
                                    Operator::Equal,
                                    Field(column(&(
                                        "parent_comments.user_id",
                                        Position::new(1, 111),
                                    ))),
                                    Field(column(&("read_ribbons.user_id", Position::new(1, 141)))),
                                )),
                                right: Box::new(ConditionExpression::Bracketed(Box::new(
                                    ConditionExpression::LogicalOp(ConditionTree {
                                        operator: Operator::And,
                                        left: Box::new(flat_condition_tree(
                                            Operator::Equal,
                                            Field(column(&(
                                                "parent_comments.user_id",
                                                Position::new(1, 171),
                                            ))),
                                            Literal(Literal::Null),
                                        )),
                                        right: Box::new(flat_condition_tree(
                                            Operator::Equal,
                                            Field(column(&(
                                                "stories.user_id",
                                                Position::new(1, 211),
                                            ))),
                                            Field(column(&(
                                                "read_ribbons.user_id",
                                                Position::new(1, 233),
                                            ))),
                                        )),
                                    }),
                                ))),
                            }),
                        ))),
                        right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                            operator: Operator::And,
                            left: Box::new(ConditionExpression::Bracketed(Box::new(
                                ConditionExpression::LogicalOp(ConditionTree {
                                    operator: Operator::Or,
                                    left: Box::new(flat_condition_tree(
                                        Operator::Equal,
                                        Field(column(&(
                                            "parent_comments.id",
                                            Position::new(1, 268),
                                        ))),
                                        Literal(Literal::Null),
                                    )),
                                    right: Box::new(flat_condition_tree(
                                        Operator::GreaterOrEqual,
                                        Field(column(&("saldo", Position::new(1, 302)))),
                                        Literal(Literal::Integer(0)),
                                    )),
                                }),
                            ))),
                            right: Box::new(flat_condition_tree(
                                Operator::Equal,
                                Field(column(&("read_ribbons.user_id", Position::new(1, 321)))),
                                Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark)),
                            )),
                        })),
                    })),
                })),
            })),
        });
        let res = res.unwrap().1;
        assert_eq!(res, expected);
    }

    #[test]
    fn not_in_comparison() {
        use ConditionBase::*;

        let qs1 = Span::new(b"id not in (1,2)");
        let res1 = condition_expr(qs1);

        let c1 = res1.unwrap().1;
        let expected1 = flat_condition_tree(
            Operator::NotIn,
            Field(column(&("id", Position::new(1, 1)))),
            LiteralList(vec![1.into(), 2.into()]),
        );
        assert_eq!(c1, expected1);

        let expected1 = "id NOT IN (1, 2)";
        assert_eq!(format!("{}", c1), expected1);
    }
}
