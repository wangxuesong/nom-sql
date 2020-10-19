use nom::character::complete::multispace0;
use std::{fmt, str};

use common::{statement_terminator, table_list};
use keywords::escape_if_keyword;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::{delimited, tuple};
use nom::IResult;
use table::Table;
use ::{Span, Position};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub pos: Position,
    pub tables: Vec<Table>,
    pub if_exists: bool,
}

impl fmt::Display for DropTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DROP TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        let ts = self
            .tables
            .iter()
            .map(|t| escape_if_keyword(&t.name))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}", ts)?;
        Ok(())
    }
}

pub fn drop_table(i: Span) -> IResult<Span, DropTableStatement> {
    let (remaining_input, (_, opt_if_exists, _, tables, _, _, _, _)) = tuple((
        tag_no_case("drop table"),
        opt(delimited(
            multispace0,
            tag_no_case("if exists"),
            multispace0,
        )),
        multispace0,
        table_list,
        multispace0,
        opt(delimited(
            multispace0,
            tag_no_case("restricted"),
            multispace0,
        )),
        opt(delimited(multispace0, tag_no_case("cascade"), multispace0)),
        statement_terminator,
    ))(i)?;

    Ok((
        remaining_input,
        DropTableStatement {
            pos: Position::from(i),
            tables,
            if_exists: opt_if_exists.is_some(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use table::Table;

    fn table_from_str(name: &str, pos: Position) -> Table {
        Table{
            pos,
            name: String::from(name),
            alias: None,
            schema: None
        }
    }

    #[test]
    fn simple_drop_table() {
        let qstring = "DROP TABLE users;";
        let res = drop_table(Span::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            DropTableStatement {
                pos: Position::new(1, 1),
                tables: vec![table_from_str("users", Position::new(1, 12))],
                if_exists: false,
            }
        );
    }

    #[test]
    fn format_drop_table() {
        let qstring = "DROP TABLE IF EXISTS users,posts;";
        let expected = "DROP TABLE IF EXISTS users, posts";
        let res = drop_table(Span::new(qstring.as_bytes()));
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }
}
