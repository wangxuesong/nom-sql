use nom::character::complete::{digit1, multispace0, multispace1};
use std::fmt;
use std::str;
use std::str::FromStr;

use column::{Column, ColumnConstraint, ColumnSpecification};
use common::{
    column_identifier_no_alias, parse_comment, schema_table_reference, sql_identifier,
    statement_terminator, type_identifier, ws_sep_comma, Literal, Real, SqlType, TableKey,
};
use compound_select::{compound_selection, CompoundSelectStatement};
use create_table_options::table_options;
use keywords::escape_if_keyword;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::combinator::{map, opt};
use nom::multi::{many0, many1};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::{AsBytes, IResult};
use order::{order_type, OrderType};
use select::{nested_selection, SelectStatement};
use table::Table;
use {Position, Span};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub pos: Position,
    pub table: Table,
    pub fields: Vec<ColumnSpecification>,
    pub keys: Option<Vec<TableKey>>,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE TABLE {} ", escape_if_keyword(&self.table.name))?;
        write!(f, "(")?;
        write!(
            f,
            "{}",
            self.fields
                .iter()
                .map(|field| format!("{}", field))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        if let Some(ref keys) = self.keys {
            write!(
                f,
                ", {}",
                keys.iter()
                    .map(|key| format!("{}", key))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        write!(f, ")")
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SelectSpecification {
    Compound(CompoundSelectStatement),
    Simple(SelectStatement),
}

impl fmt::Display for SelectSpecification {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SelectSpecification::Compound(ref csq) => write!(f, "{}", csq),
            SelectSpecification::Simple(ref sq) => write!(f, "{}", sq),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateViewStatement {
    pub pos: Position,
    pub name: String,
    pub fields: Vec<Column>,
    pub definition: Box<SelectSpecification>,
}

impl fmt::Display for CreateViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE VIEW {} ", escape_if_keyword(&self.name))?;
        if !self.fields.is_empty() {
            write!(f, "(")?;
            write!(
                f,
                "{}",
                self.fields
                    .iter()
                    .map(|field| format!("{}", field))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, ") ")?;
        }
        write!(f, "AS ")?;
        write!(f, "{}", self.definition)
    }
}

// MySQL grammar element for index column definition (§13.1.18, index_col_name)
pub fn index_col_name(i: Span) -> IResult<Span, (Column, Option<u16>, Option<OrderType>)> {
    let (remaining_input, (column, len_u8, order)) = tuple((
        terminated(column_identifier_no_alias, multispace0),
        opt(delimited(tag("("), digit1, tag(")"))),
        opt(order_type),
    ))(i)?;
    let len = len_u8.map(|l| u16::from_str(str::from_utf8(l.fragment()).unwrap()).unwrap());

    Ok((remaining_input, (column, len, order)))
}

// Helper for list of index columns
pub fn index_col_list(i: Span) -> IResult<Span, Vec<Column>> {
    many0(map(
        terminated(index_col_name, opt(ws_sep_comma)),
        // XXX(malte): ignores length and order
        |e| e.0,
    ))(i)
}

// Parse rule for an individual key specification.
pub fn key_specification(i: Span) -> IResult<Span, TableKey> {
    alt((full_text_key, primary_key, unique, key_or_index))(i)
}

fn full_text_key(i: Span) -> IResult<Span, TableKey> {
    let (remaining_input, (_, _, _, _, name, _, columns)) = tuple((
        tag_no_case("fulltext"),
        multispace1,
        alt((tag_no_case("key"), tag_no_case("index"))),
        multispace1,
        opt(sql_identifier),
        multispace0,
        delimited(
            tag("("),
            delimited(multispace0, index_col_list, multispace0),
            tag(")"),
        ),
    ))(i)?;

    match name {
        Some(name) => {
            let n = String::from_utf8(name.as_bytes().to_vec()).unwrap();
            Ok((remaining_input, TableKey::FulltextKey(Some(n), columns)))
        }
        None => Ok((remaining_input, TableKey::FulltextKey(None, columns))),
    }
}

fn primary_key(i: Span) -> IResult<Span, TableKey> {
    let (remaining_input, (_, _, columns, _)) = tuple((
        tag_no_case("primary key"),
        multispace0,
        delimited(
            tag("("),
            delimited(multispace0, index_col_list, multispace0),
            tag(")"),
        ),
        opt(map(
            preceded(multispace1, tag_no_case("auto_increment")),
            |_| (),
        )),
    ))(i)?;

    Ok((remaining_input, TableKey::PrimaryKey(columns)))
}

fn unique(i: Span) -> IResult<Span, TableKey> {
    // TODO: add branching to correctly parse whitespace after `unique`
    let (remaining_input, (_, _, _, name, _, columns)) = tuple((
        tag_no_case("unique"),
        opt(preceded(
            multispace1,
            alt((tag_no_case("key"), tag_no_case("index"))),
        )),
        multispace0,
        opt(sql_identifier),
        multispace0,
        delimited(
            tag("("),
            delimited(multispace0, index_col_list, multispace0),
            tag(")"),
        ),
    ))(i)?;

    match name {
        Some(name) => {
            let n = String::from_utf8(name.as_bytes().to_vec()).unwrap();
            Ok((remaining_input, TableKey::UniqueKey(Some(n), columns)))
        }
        None => Ok((remaining_input, TableKey::UniqueKey(None, columns))),
    }
}

fn key_or_index(i: Span) -> IResult<Span, TableKey> {
    let (remaining_input, (_, _, name, _, columns)) = tuple((
        alt((tag_no_case("key"), tag_no_case("index"))),
        multispace0,
        sql_identifier,
        multispace0,
        delimited(
            tag("("),
            delimited(multispace0, index_col_list, multispace0),
            tag(")"),
        ),
    ))(i)?;

    let n = String::from_utf8(name.as_bytes().to_vec()).unwrap();
    Ok((remaining_input, TableKey::Key(n, columns)))
}

// Parse rule for a comma-separated list.
pub fn key_specification_list(i: Span) -> IResult<Span, Vec<TableKey>> {
    many1(terminated(key_specification, opt(ws_sep_comma)))(i)
}

fn field_specification(i: Span) -> IResult<Span, ColumnSpecification> {
    let (remaining_input, (column, field_type, constraints, comment, _)) = tuple((
        column_identifier_no_alias,
        opt(delimited(multispace1, type_identifier, multispace0)),
        many0(column_constraint),
        opt(parse_comment),
        opt(ws_sep_comma),
    ))(i)?;

    let sql_type = match field_type {
        None => SqlType::Text,
        Some(ref t) => t.clone(),
    };
    Ok((
        remaining_input,
        ColumnSpecification {
            column,
            sql_type,
            constraints: constraints.into_iter().filter_map(|m| m).collect(),
            comment,
        },
    ))
}

// Parse rule for a comma-separated list.
pub fn field_specification_list(i: Span) -> IResult<Span, Vec<ColumnSpecification>> {
    many1(field_specification)(i)
}

// Parse rule for a column definition constraint.
pub fn column_constraint(i: Span) -> IResult<Span, Option<ColumnConstraint>> {
    let not_null = map(
        delimited(multispace0, tag_no_case("not null"), multispace0),
        |_| Some(ColumnConstraint::NotNull),
    );
    let null = map(
        delimited(multispace0, tag_no_case("null"), multispace0),
        |_| None,
    );
    let auto_increment = map(
        delimited(multispace0, tag_no_case("auto_increment"), multispace0),
        |_| Some(ColumnConstraint::AutoIncrement),
    );
    let primary_key = map(
        delimited(multispace0, tag_no_case("primary key"), multispace0),
        |_| Some(ColumnConstraint::PrimaryKey),
    );
    let unique = map(
        delimited(multispace0, tag_no_case("unique"), multispace0),
        |_| Some(ColumnConstraint::Unique),
    );
    let character_set = map(
        preceded(
            delimited(multispace0, tag_no_case("character set"), multispace1),
            sql_identifier,
        ),
        |cs| {
            let char_set = str::from_utf8(cs.fragment()).unwrap().to_owned();
            Some(ColumnConstraint::CharacterSet(char_set))
        },
    );
    let collate = map(
        preceded(
            delimited(multispace0, tag_no_case("collate"), multispace1),
            sql_identifier,
        ),
        |c| {
            let collation = str::from_utf8(c.fragment()).unwrap().to_owned();
            Some(ColumnConstraint::Collation(collation))
        },
    );

    alt((
        not_null,
        null,
        auto_increment,
        default,
        primary_key,
        unique,
        character_set,
        collate,
    ))(i)
}

fn fixed_point(i: Span) -> IResult<Span, Literal> {
    let (remaining_input, (i, _, f)) = tuple((digit1, tag("."), digit1))(i)?;

    Ok((
        remaining_input,
        Literal::FixedPoint(Real {
            integral: i32::from_str(str::from_utf8(i.fragment()).unwrap()).unwrap(),
            fractional: i32::from_str(str::from_utf8(f.fragment()).unwrap()).unwrap(),
        }),
    ))
}

fn default(i: Span) -> IResult<Span, Option<ColumnConstraint>> {
    let (remaining_input, (_, _, _, def, _)) = tuple((
        multispace0,
        tag_no_case("default"),
        multispace1,
        alt((
            map(delimited(tag("'"), take_until("'"), tag("'")), |s: Span| {
                Literal::String(String::from_utf8(s.as_bytes().to_vec()).unwrap())
            }),
            fixed_point,
            map(digit1, |d: Span| {
                let d_i64 = i64::from_str(str::from_utf8(d.as_bytes()).unwrap()).unwrap();
                Literal::Integer(d_i64)
            }),
            map(tag("''"), |_| Literal::String(String::from(""))),
            map(tag_no_case("null"), |_| Literal::Null),
            map(tag_no_case("current_timestamp"), |_| {
                Literal::CurrentTimestamp
            }),
        )),
        multispace0,
    ))(i)?;

    Ok((remaining_input, Some(ColumnConstraint::DefaultValue(def))))
}

// Parse rule for a SQL CREATE TABLE query.
// TODO(malte): support types, TEMPORARY tables, IF NOT EXISTS, AS stmt
pub fn creation(i: Span) -> IResult<Span, CreateTableStatement> {
    let (remaining_input, (_, _, _, _, table, _, _, _, fields_list, _, keys_list, _, _, _, _, _)) =
        tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            schema_table_reference,
            multispace0,
            tag("("),
            multispace0,
            field_specification_list,
            multispace0,
            opt(key_specification_list),
            multispace0,
            tag(")"),
            multispace0,
            table_options,
            statement_terminator,
        ))(i)?;

    // "table AS alias" isn't legal in CREATE statements
    assert!(table.alias.is_none());
    // attach table names to columns:
    let fields = fields_list
        .into_iter()
        .map(|field| {
            let column = Column {
                table: Some(table.name.clone()),
                ..field.column
            };

            ColumnSpecification { column, ..field }
        })
        .collect();

    // and to keys:
    let keys = keys_list.and_then(|ks| {
        Some(
            ks.into_iter()
                .map(|key| {
                    let attach_names = |columns: Vec<Column>| {
                        columns
                            .into_iter()
                            .map(|column| Column {
                                table: Some(table.name.clone()),
                                ..column
                            })
                            .collect()
                    };

                    match key {
                        TableKey::PrimaryKey(columns) => {
                            TableKey::PrimaryKey(attach_names(columns))
                        }
                        TableKey::UniqueKey(name, columns) => {
                            TableKey::UniqueKey(name, attach_names(columns))
                        }
                        TableKey::FulltextKey(name, columns) => {
                            TableKey::FulltextKey(name, attach_names(columns))
                        }
                        TableKey::Key(name, columns) => TableKey::Key(name, attach_names(columns)),
                    }
                })
                .collect(),
        )
    });

    Ok((
        remaining_input,
        CreateTableStatement {
            pos: Position::from(i),
            table,
            fields,
            keys,
        },
    ))
}

// Parse rule for a SQL CREATE VIEW query.
pub fn view_creation(i: Span) -> IResult<Span, CreateViewStatement> {
    let (remaining_input, (_, _, _, _, name_slice, _, _, _, def, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("view"),
        multispace1,
        sql_identifier,
        multispace1,
        tag_no_case("as"),
        multispace1,
        alt((
            map(compound_selection, |s| SelectSpecification::Compound(s)),
            map(nested_selection, |s| SelectSpecification::Simple(s)),
        )),
        statement_terminator,
    ))(i)?;

    let name = String::from_utf8(name_slice.as_bytes().to_vec()).unwrap();
    let fields = vec![]; // TODO(malte): support
    let definition = Box::new(def);

    Ok((
        remaining_input,
        CreateViewStatement {
            pos: Position::from(i),
            name,
            fields,
            definition,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use table::Table;

    fn table_from_str(name: &str, pos: Position) -> Table {
        Table {
            pos,
            name: String::from(name),
            alias: None,
            schema: None,
        }
    }

    fn table_from_schema(name: (&str, &str), pos: Position) -> Table {
        let mut table = Table::from(name);
        table.pos = pos;
        table
    }

    #[test]
    fn sql_types() {
        let type0 = "bigint(20)";
        let type1 = "varchar(255) binary";
        let type2 = "bigint(20) unsigned";
        let type3 = "bigint(20) signed";

        let res = type_identifier(Span::new(type0.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::Bigint(20));
        let res = type_identifier(Span::new(type1.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::Varchar(255));
        let res = type_identifier(Span::new(type2.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::UnsignedBigint(20));
        let res = type_identifier(Span::new(type3.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::Bigint(20));
        let res = type_identifier(Span::new(type2.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::UnsignedBigint(20));
    }

    #[test]
    fn field_spec() {
        // N.B. trailing comma here because field_specification_list! doesn't handle the eof case
        // because it is never validly the end of a query
        let qstring = "id bigint(20), name varchar(255),";

        let res = field_specification_list(Span::new(qstring.as_bytes()));
        let mut column = Column::from("id");
        column.pos = Position::new(1, 1);
        let mut column1 = Column::from("name");
        column1.pos = Position::new(1, 16);
        assert_eq!(
            res.unwrap().1,
            vec![
                ColumnSpecification::new(column, SqlType::Bigint(20)),
                ColumnSpecification::new(column1, SqlType::Varchar(255)),
            ]
        );
    }

    #[test]
    fn simple_create() {
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255));";

        let res = creation(Span::new(qstring.as_bytes()));
        let mut column = Column::from("users.id");
        column.pos = Position::new(1, 21);
        let mut column1 = Column::from("users.name");
        column1.pos = Position::new(1, 36);
        let mut column2 = Column::from("users.email");
        column2.pos = Position::new(1, 55);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_str("users", Position::new(1, 14)),
                fields: vec![
                    ColumnSpecification::new(column, SqlType::Bigint(20)),
                    ColumnSpecification::new(column1, SqlType::Varchar(255)),
                    ColumnSpecification::new(column2, SqlType::Varchar(255)),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn create_without_space_after_tablename() {
        let qstring = "CREATE TABLE t(x integer);";
        let res = creation(Span::new(qstring.as_bytes()));
        let mut column = Column::from("t.x");
        column.pos = Position::new(1, 16);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_str("t", Position::new(1, 14)),
                fields: vec![ColumnSpecification::new(column, SqlType::Int(32)),],
                ..Default::default()
            }
        );
    }

    #[test]
    fn create_tablename_with_schema() {
        let qstring = "CREATE TABLE db1.t(x integer);";
        let res = creation(Span::new(qstring.as_bytes()));
        let mut column = Column::from("t.x");
        column.pos = Position::new(1, 20);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_schema(("db1", "t"), Position::new(1, 14)),
                fields: vec![ColumnSpecification::new(column, SqlType::Int(32)),],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mediawiki_create() {
        let qstring = "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip \
                       varchar(40) NOT NULL default '') TYPE=MyISAM;";
        let res = creation(Span::new(qstring.as_bytes()));
        let mut column = Column::from("user_newtalk.user_id");
        column.pos = Position::new(1, 30);
        let mut column1 = Column::from("user_newtalk.user_ip");
        column1.pos = Position::new(1, 68);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_str("user_newtalk", Position::new(1, 14)),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        column,
                        SqlType::Int(5),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::DefaultValue(Literal::String(String::from("0"))),
                        ],
                    ),
                    ColumnSpecification::with_constraints(
                        column1,
                        SqlType::Varchar(40),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::DefaultValue(Literal::String(String::from(""))),
                        ],
                    ),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mediawiki_create2() {
        let qstring = "CREATE TABLE `user` (
                        user_id int unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        user_name varchar(255) binary NOT NULL default '',
                        user_real_name varchar(255) binary NOT NULL default '',
                        user_password tinyblob NOT NULL,
                        user_newpassword tinyblob NOT NULL,
                        user_newpass_time binary(14),
                        user_email tinytext NOT NULL,
                        user_touched binary(14) NOT NULL default '',
                        user_token binary(32) NOT NULL default '',
                        user_email_authenticated binary(14),
                        user_email_token binary(32),
                        user_email_token_expires binary(14),
                        user_registration binary(14),
                        user_editcount int,
                        user_password_expires varbinary(14) DEFAULT NULL
                       ) ENGINE=, DEFAULT CHARSET=utf8";
        creation(Span::new(qstring.as_bytes())).unwrap();
    }

    #[test]
    fn mediawiki_create3() {
        let qstring = "CREATE TABLE `interwiki` (
 iw_prefix varchar(32) NOT NULL,
 iw_url blob NOT NULL,
 iw_api blob NOT NULL,
 iw_wikiid varchar(64) NOT NULL,
 iw_local bool NOT NULL,
 iw_trans tinyint NOT NULL default 0
 ) ENGINE=, DEFAULT CHARSET=utf8";
        creation(Span::new(qstring.as_bytes())).unwrap();
    }

    #[test]
    fn mediawiki_externallinks() {
        let qstring = "CREATE TABLE `externallinks` (
          `el_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `el_from` int(8) unsigned NOT NULL DEFAULT '0',
          `el_from_namespace` int(11) NOT NULL DEFAULT '0',
          `el_to` blob NOT NULL,
          `el_index` blob NOT NULL,
          `el_index_60` varbinary(60) NOT NULL,
          PRIMARY KEY (`el_id`),
          KEY `el_from` (`el_from`,`el_to`(40)),
          KEY `el_to` (`el_to`(60),`el_from`),
          KEY `el_index` (`el_index`(60)),
          KEY `el_backlinks_to` (`el_from_namespace`,`el_to`(60),`el_from`),
          KEY `el_index_60` (`el_index_60`,`el_id`),
          KEY `el_from_index_60` (`el_from`,`el_index_60`,`el_id`)
        )";
        creation(Span::new(qstring.as_bytes())).unwrap();
    }

    #[test]
    fn keys() {
        // simple primary key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       PRIMARY KEY (id));";

        let res = creation(Span::new(qstring.as_bytes()));
        let mut column = Column::from("users.id");
        column.pos = Position::new(1, 21);
        let mut column1 = Column::from("users.name");
        column1.pos = Position::new(1, 36);
        let mut column2 = Column::from("users.email");
        column2.pos = Position::new(1, 55);
        let mut column3 = Column::from("users.id");
        column3.pos = Position::new(1, 88);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_str("users", Position::new(1, 14)),
                fields: vec![
                    ColumnSpecification::new(column, SqlType::Bigint(20)),
                    ColumnSpecification::new(column1, SqlType::Varchar(255)),
                    ColumnSpecification::new(column2, SqlType::Varchar(255)),
                ],
                keys: Some(vec![TableKey::PrimaryKey(vec![column3])]),
                ..Default::default()
            }
        );

        // named unique key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       UNIQUE KEY id_k (id));";

        let res = creation(Span::new(qstring.as_bytes()));
        let mut column4 = Column::from("users.id");
        column4.pos = Position::new(1, 21);
        let mut column5 = Column::from("users.name");
        column5.pos = Position::new(1, 36);
        let mut column6 = Column::from("users.email");
        column6.pos = Position::new(1, 55);
        let mut column7 = Column::from("users.id");
        column7.pos = Position::new(1, 92);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_str("users", Position::new(1, 14)),
                fields: vec![
                    ColumnSpecification::new(column4, SqlType::Bigint(20)),
                    ColumnSpecification::new(column5, SqlType::Varchar(255)),
                    ColumnSpecification::new(column6, SqlType::Varchar(255)),
                ],
                keys: Some(vec![TableKey::UniqueKey(
                    Some(String::from("id_k")),
                    vec![column7],
                ),]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn django_create() {
        let qstring = "CREATE TABLE `django_admin_log` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `action_time` datetime NOT NULL,
                       `user_id` integer NOT NULL,
                       `content_type_id` integer,
                       `object_id` longtext,
                       `object_repr` varchar(200) NOT NULL,
                       `action_flag` smallint UNSIGNED NOT NULL,
                       `change_message` longtext NOT NULL);";
        let res = creation(Span::new(qstring.as_bytes()));
        let mut column = Column::from("django_admin_log.id");
        column.pos = Position::new(2, 24);
        let mut column1 = Column::from("django_admin_log.action_time");
        column1.pos = Position::new(3, 24);
        let mut column2 = Column::from("django_admin_log.user_id");
        column2.pos = Position::new(4, 24);
        let mut column3 = Column::from("django_admin_log.content_type_id");
        column3.pos = Position::new(5, 24);
        let mut column4 = Column::from("django_admin_log.object_id");
        column4.pos = Position::new(6, 24);
        let mut column5 = Column::from("django_admin_log.object_repr");
        column5.pos = Position::new(7, 24);
        let mut column6 = Column::from("django_admin_log.action_flag");
        column6.pos = Position::new(8, 24);
        let mut column7 = Column::from("django_admin_log.change_message");
        column7.pos = Position::new(9, 24);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_str("django_admin_log", Position::new(1, 14)),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        column,
                        SqlType::Int(32),
                        vec![
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::NotNull,
                            ColumnConstraint::PrimaryKey,
                        ],
                    ),
                    ColumnSpecification::with_constraints(
                        column1,
                        SqlType::DateTime(0),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        column2,
                        SqlType::Int(32),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::new(column3, SqlType::Int(32),),
                    ColumnSpecification::new(column4, SqlType::Longtext,),
                    ColumnSpecification::with_constraints(
                        column5,
                        SqlType::Varchar(200),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        column6,
                        SqlType::UnsignedInt(32),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        column7,
                        SqlType::Longtext,
                        vec![ColumnConstraint::NotNull],
                    ),
                ],
                ..Default::default()
            }
        );

        let qstring = "CREATE TABLE `auth_group` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `name` varchar(80) NOT NULL UNIQUE)";
        let res = creation(Span::new(qstring.as_bytes()));
        let mut column8 = Column::from("auth_group.id");
        column8.pos = Position::new(2, 24);
        let mut column9 = Column::from("auth_group.name");
        column9.pos = Position::new(3, 24);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_str("auth_group", Position::new(1, 14)),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        column8,
                        SqlType::Int(32),
                        vec![
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::NotNull,
                            ColumnConstraint::PrimaryKey,
                        ],
                    ),
                    ColumnSpecification::with_constraints(
                        column9,
                        SqlType::Varchar(80),
                        vec![ColumnConstraint::NotNull, ColumnConstraint::Unique],
                    ),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn format_create() {
        let qstring = "CREATE TABLE `auth_group` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `name` varchar(80) NOT NULL UNIQUE)";
        // TODO(malte): INTEGER isn't quite reflected right here, perhaps
        let expected = "CREATE TABLE auth_group (\
                        id INT(32) AUTO_INCREMENT NOT NULL PRIMARY KEY, \
                        name VARCHAR(80) NOT NULL UNIQUE)";
        let res = creation(Span::new(qstring.as_bytes()));
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }

    #[test]
    fn simple_create_view() {
        use common::{FieldDefinitionExpression, Operator};
        use condition::{ConditionBase, ConditionExpression, ConditionTree};

        let qstring = "CREATE VIEW v AS SELECT * FROM users WHERE username = \"bob\";";

        let res = view_creation(Span::new(qstring.as_bytes()));
        let mut column: Column = "username".into();
        column.pos = Position::new(1, 44);
        assert_eq!(
            res.unwrap().1,
            CreateViewStatement {
                pos: Position::new(1, 1),
                name: String::from("v"),
                fields: vec![],
                definition: Box::new(SelectSpecification::Simple(SelectStatement {
                    pos: Position::new(1, 18),
                    tables: vec![table_from_str("users", Position::new(1, 32))],
                    fields: vec![FieldDefinitionExpression::All],
                    where_clause: Some(ConditionExpression::ComparisonOp(ConditionTree {
                        left: Box::new(ConditionExpression::Base(ConditionBase::Field(column))),
                        right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                            Literal::String("bob".into())
                        ))),
                        operator: Operator::Equal,
                    })),
                    ..Default::default()
                })),
            }
        );
    }

    #[test]
    fn compound_create_view() {
        use common::FieldDefinitionExpression;
        use compound_select::{CompoundSelectOperator, CompoundSelectStatement};

        let qstring = "CREATE VIEW v AS SELECT * FROM users UNION SELECT * FROM old_users;";

        let res = view_creation(Span::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            CreateViewStatement {
                pos: Position::new(1, 1),
                name: String::from("v"),
                fields: vec![],
                definition: Box::new(SelectSpecification::Compound(CompoundSelectStatement {
                    pos: Position::new(1, 18),
                    selects: vec![
                        (
                            None,
                            SelectStatement {
                                pos: Position::new(1, 18),
                                tables: vec![table_from_str("users", Position::new(1, 32))],
                                fields: vec![FieldDefinitionExpression::All],
                                ..Default::default()
                            },
                        ),
                        (
                            Some(CompoundSelectOperator::DistinctUnion),
                            SelectStatement {
                                pos: Position::new(1, 44),
                                tables: vec![table_from_str("old_users", Position::new(1, 58))],
                                fields: vec![FieldDefinitionExpression::All],
                                ..Default::default()
                            },
                        ),
                    ],
                    order: None,
                    limit: None,
                })),
            }
        );
    }

    #[test]
    fn format_create_view() {
        let qstring = "CREATE VIEW `v` AS SELECT * FROM `t`;";
        let expected = "CREATE VIEW v AS SELECT * FROM t";
        let res = view_creation(Span::new(qstring.as_bytes()));
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }

    #[test]
    fn lobsters_indexes() {
        let qstring = "CREATE TABLE `comments` (
            `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `hat_id` int,
            fulltext INDEX `index_comments_on_comment`  (`comment`),
            INDEX `confidence_idx`  (`confidence`),
            UNIQUE INDEX `short_id`  (`short_id`),
            INDEX `story_id_short_id`  (`story_id`, `short_id`),
            INDEX `thread_id`  (`thread_id`),
            INDEX `index_comments_on_user_id`  (`user_id`))
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
        let res = creation(Span::new(qstring.as_bytes()));
        let mut column = Column::from("comments.id");
        column.pos = Position::new(2, 13);
        let mut column1 = Column::from("comments.hat_id");
        column1.pos = Position::new(3, 13);
        let mut column2 = Column::from("comments.comment");
        column2.pos = Position::new(4, 58);
        let mut column3 = Column::from("comments.confidence");
        column3.pos = Position::new(5, 38);
        let mut column4 = Column::from("comments.short_id");
        column4.pos = Position::new(6, 39);
        let mut column5 = Column::from("comments.story_id");
        column5.pos = Position::new(7, 41);
        let mut column6 = Column::from("comments.short_id");
        column6.pos = Position::new(7, 53);
        let mut column7 = Column::from("comments.thread_id");
        column7.pos = Position::new(8, 33);
        let mut column8 = Column::from("comments.user_id");
        column8.pos = Position::new(9, 49);
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                pos: Position::new(1, 1),
                table: table_from_str("comments", Position::new(1, 14)),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        column,
                        SqlType::UnsignedInt(32),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::PrimaryKey,
                        ],
                    ),
                    ColumnSpecification::new(column1, SqlType::Int(32),),
                ],
                keys: Some(vec![
                    TableKey::FulltextKey(Some("index_comments_on_comment".into()), vec![column2]),
                    TableKey::Key("confidence_idx".into(), vec![column3]),
                    TableKey::UniqueKey(Some("short_id".into()), vec![column4]),
                    TableKey::Key("story_id_short_id".into(), vec![column5, column6]),
                    TableKey::Key("thread_id".into(), vec![column7]),
                    TableKey::Key("index_comments_on_user_id".into(), vec![column8]),
                ]),
            }
        );
    }
}
