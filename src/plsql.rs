use arithmetic::arithmetic_base;
use common::{sql_identifier, statement_terminator, ws_sep_comma};
use condition::condition_expr;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::multi::{many0, many1};
use nom::sequence::{preceded, tuple};
use nom::IResult;
use ArithmeticBase;
use ConditionExpression;
use {Position, Span};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ParameterDeclaration {
    pub pos: Position,
    pub name: String,
    pub data_type: String,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ParameterDeclarations {}

fn parameter_declaration(i: Span) -> IResult<Span, ParameterDeclaration> {
    let (remaining_input, (name, _, dt, _)) = tuple((
        sql_identifier,
        multispace1,
        sql_identifier,
        opt(ws_sep_comma),
    ))(i)?;
    Ok((
        remaining_input,
        ParameterDeclaration {
            pos: Position::from_span(name),
            name: String::from(std::str::from_utf8(name.fragment()).unwrap()),
            data_type: String::from(std::str::from_utf8(dt.fragment()).unwrap()),
        },
    ))
}

fn parameter_declarations(i: Span) -> IResult<Span, Vec<ParameterDeclaration>> {
    let (remaining_input, (_, _, parameters, _, _)) = tuple((
        tag("("),
        multispace0,
        many1(parameter_declaration),
        multispace0,
        tag(")"),
    ))(i)?;
    Ok((remaining_input, parameters))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Declaration {
    Variable(PlSqlVariable),
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct PlSqlVariable {
    pub pos: Position,
    pub name: String,
    pub data_type: String,
}

fn declarations(i: Span) -> IResult<Span, Declaration> {
    let (remaining_input, (name, _, dt, _, _)) = tuple((
        sql_identifier,
        multispace1,
        sql_identifier,
        opt(tuple((
            multispace1,
            tag_no_case(":="),
            multispace1,
            arithmetic_base,
        ))),
        statement_terminator,
    ))(i)?;
    Ok((
        remaining_input,
        Declaration::Variable(PlSqlVariable {
            pos: Position::from_span(name),
            name: String::from(std::str::from_utf8(name.fragment()).unwrap()),
            data_type: String::from(std::str::from_utf8(dt.fragment()).unwrap()),
        }),
    ))
}

fn declare_section(i: Span) -> IResult<Span, Vec<Declaration>> {
    many1(declarations)(i)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum PlSqlStatement {
    Assignment(AssignmentStatement),
    If(IfStatement),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct AssignmentStatement {
    pub pos: Position,
    pub left: String,
    pub expr: ArithmeticBase,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IfStatement {
    pub pos: Position,
    pub cond: ConditionExpression,
    pub then_body: Option<Vec<PlSqlStatement>>,
    pub else_body: Option<Vec<PlSqlStatement>>,
}

fn assignment_statement(i: Span) -> IResult<Span, PlSqlStatement> {
    map(
        tuple((
            sql_identifier,
            multispace0,
            tag_no_case(":="),
            multispace0,
            arithmetic_base,
            statement_terminator,
        )),
        |tup| {
            PlSqlStatement::Assignment(AssignmentStatement {
                pos: Position::from_span(tup.0),
                left: String::from(std::str::from_utf8(tup.0.fragment()).unwrap()),
                expr: tup.4,
            })
        },
    )(i)
}

fn if_statement(i: Span) -> IResult<Span, PlSqlStatement> {
    map(
        tuple((
            tag_no_case("if"),
            multispace1,
            condition_expr,
            multispace1,
            tag_no_case("then"),
            multispace1,
            opt(many1(plsql_statement)),
            multispace0,
            opt(preceded(
                tuple((tag_no_case("else"), multispace1)),
                many0(plsql_statement),
            )),
            tag_no_case("end"),
            multispace1,
            tag_no_case("if"),
            statement_terminator,
        )),
        |tup| {
            PlSqlStatement::If(IfStatement {
                pos: Position::from_span(tup.0),
                cond: tup.2,
                then_body: tup.6,
                else_body: tup.8,
            })
        },
    )(i)
}

fn plsql_statement(i: Span) -> IResult<Span, PlSqlStatement> {
    alt((assignment_statement, if_statement))(i)
}

fn plsql_procedure_body(i: Span) -> IResult<Span, Vec<PlSqlStatement>> {
    let (remaining_input, (_, _, body, _, _, _)) = tuple((
        tag_no_case("begin"),
        multispace1,
        many0(plsql_statement),
        multispace0,
        tag_no_case("end"),
        statement_terminator,
    ))(i)?;
    Ok((remaining_input, body))
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Procedure {
    pub pos: Position,
    pub name: String,
    pub parameters: Option<Vec<ParameterDeclaration>>,
    pub declarations: Option<Vec<Declaration>>,
    pub statements: Option<Vec<PlSqlStatement>>,
}

pub fn or_replace(i: Span) -> IResult<Span, bool> {
    let (remaining_input, is_replace) = opt(tuple((
        tag_no_case("or"),
        multispace1,
        tag_no_case("replace"),
        multispace1,
    )))(i)?;
    match is_replace {
        Some(_) => Ok((remaining_input, true)),
        None => Ok((remaining_input, false)),
    }
}

pub fn plsql_procedure_source(i: Span) -> IResult<Span, Procedure> {
    let (remaining_input, (name, _, paras, _, _, _, declares, body)) = tuple((
        sql_identifier,
        multispace0,
        opt(parameter_declarations),
        multispace0,
        alt((tag_no_case("is"), tag_no_case("as"))),
        multispace1,
        opt(declare_section),
        opt(plsql_procedure_body),
    ))(i)?;
    Ok((
        remaining_input,
        Procedure {
            pos: Position::from_span(name),
            name: String::from(std::str::from_utf8(name.fragment()).unwrap()),
            parameters: paras,
            declarations: declares,
            statements: body,
        },
    ))
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateProcedureStatement {
    pub is_replace: bool,
    pub proc: Procedure,
}

pub fn create_procedure(i: Span) -> IResult<Span, CreateProcedureStatement> {
    let (remaining_input, (_, _, is_replace, _, _, proc)) = tuple((
        tag_no_case("create"),
        multispace1,
        or_replace,
        tag_no_case("procedure"),
        multispace1,
        plsql_procedure_source,
    ))(i)?;

    // proc.parameters = params;
    Ok((
        remaining_input,
        CreateProcedureStatement { is_replace, proc },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::Operator::Greater;
    use plsql::Declaration::Variable;
    use ConditionTree;
    use Literal;
    use {Column, ConditionBase};

    #[test]
    fn create_procedure_test() {
        let sql = "CREATE OR REPLACE PROCEDURE test
(
    sales NUMBER,
    quota NUMBER,
    emp_id NUMBER
)
IS
    bonus NUMBER := 0;
BEGIN
    IF sales > quota THEN
        bonus := 50;
    ELSE
        bonus := 0;
    END IF;
END;";
        let res = create_procedure(Span::new(sql.as_bytes()));
        let (s, stat) = res.unwrap();
        assert_eq!(
            stat,
            CreateProcedureStatement {
                is_replace: true,
                proc: Procedure {
                    pos: Position::new(1, 29),
                    name: "test".to_string(),
                    parameters: Some(vec![
                        ParameterDeclaration {
                            pos: Position::new(3, 5),
                            name: "sales".to_string(),
                            data_type: "NUMBER".to_string()
                        },
                        ParameterDeclaration {
                            pos: Position::new(4, 5),
                            name: "quota".to_string(),
                            data_type: "NUMBER".to_string()
                        },
                        ParameterDeclaration {
                            pos: Position::new(5, 5),
                            name: "emp_id".to_string(),
                            data_type: "NUMBER".to_string()
                        }
                    ]),
                    declarations: Some(vec![Variable(PlSqlVariable {
                        pos: Position::new(8, 5),
                        name: "bonus".to_string(),
                        data_type: "NUMBER".to_string()
                    })]),
                    statements: Some(vec![PlSqlStatement::If(IfStatement {
                        pos: Position::new(10, 5),
                        cond: ConditionExpression::ComparisonOp(ConditionTree {
                            operator: Greater,
                            left: Box::new(ConditionExpression::Base(ConditionBase::Field(
                                Column {
                                    pos: Position {
                                        line: 10,
                                        offset: 8,
                                    },
                                    name: "sales".to_string(),
                                    alias: None,
                                    table: None,
                                    function: None,
                                },
                            ),)),
                            right: Box::new(ConditionExpression::Base(ConditionBase::Field(
                                Column {
                                    pos: Position {
                                        line: 10,
                                        offset: 16,
                                    },
                                    name: "quota".to_string(),
                                    alias: None,
                                    table: None,
                                    function: None,
                                },
                            ),)),
                        }),
                        then_body: Some(vec![PlSqlStatement::Assignment(AssignmentStatement {
                            pos: Position::new(11, 9),
                            left: "bonus".to_string(),
                            expr: ArithmeticBase::Scalar(Literal::Integer(50))
                        })]),
                        else_body: Some(vec![PlSqlStatement::Assignment(AssignmentStatement {
                            pos: Position::new(13, 9),
                            left: "bonus".to_string(),
                            expr: ArithmeticBase::Scalar(Literal::Integer(0))
                        })])
                    })]),
                }
            }
        );
        assert_eq!(std::str::from_utf8(s.fragment()).unwrap(), "");
    }
}
