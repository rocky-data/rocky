//! Token types for the Rocky DSL lexer.

use logos::Logos;

/// Tokens produced by the Rocky DSL lexer.
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r]+")]
#[logos(skip(r"--[^\n]*", allow_greedy = true))]
pub enum Token {
    // Keywords
    #[token("let")]
    Let,
    #[token("from")]
    From,
    #[token("where")]
    Where,
    #[token("group")]
    Group,
    #[token("derive")]
    Derive,
    #[token("select")]
    Select,
    #[token("join")]
    Join,
    #[token("window")]
    Window,
    #[token("sort")]
    Sort,
    #[token("take")]
    Take,
    #[token("distinct")]
    Distinct,
    #[token("union")]
    Union,
    #[token("replicate")]
    Replicate,
    #[token("check")]
    Check,
    #[token("as")]
    As,
    #[token("on")]
    On,
    #[token("by")]
    By,
    #[token("order")]
    Order,
    #[token("keep")]
    Keep,
    #[token("asc")]
    Asc,
    #[token("desc")]
    Desc,
    #[token("and")]
    And,
    #[token("or")]
    Or,
    #[token("not")]
    Not,
    #[token("is")]
    Is,
    #[token("null")]
    Null,
    #[token("in")]
    In,
    #[token("true")]
    True,
    #[token("false")]
    False,
    #[token("match")]
    Match,
    #[token("over")]
    Over,
    #[token("partition")]
    Partition,
    #[token("rows")]
    Rows,
    #[token("range")]
    Range,
    #[token("unbounded")]
    Unbounded,
    #[token("current")]
    Current,

    // Symbols
    #[token("..")]
    DotDot,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token(":")]
    Colon,
    #[token(",")]
    Comma,
    #[token(".")]
    Dot,
    #[token("*")]
    Star,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("/")]
    Slash,
    #[token("%")]
    Percent,

    // Comparison
    #[token("==")]
    Eq,
    #[token("!=")]
    Neq,
    #[token(">=")]
    Gte,
    #[token("<=")]
    Lte,
    #[token(">")]
    Gt,
    #[token("<")]
    Lt,

    // Assignment / Pipe
    #[token("=")]
    Assign,
    #[token("|")]
    Pipe,

    // Other
    #[token("=>")]
    FatArrow,
    #[token("_")]
    Underscore,
    #[token("\n")]
    Newline,

    // Literals
    #[regex(r#""[^"]*""#, |lex| lex.slice()[1..lex.slice().len()-1].to_string())]
    #[regex(r#"'[^']*'"#, |lex| lex.slice()[1..lex.slice().len()-1].to_string())]
    StringLit(String),

    #[regex(r"[0-9][0-9_]*(\.[0-9][0-9_]*)?", |lex| lex.slice().replace('_', ""))]
    NumberLit(String),

    #[regex(r"@[0-9]{4}-[0-9]{2}-[0-9]{2}(T[0-9]{2}:[0-9]{2}:[0-9]{2}Z?)?", |lex| lex.slice()[1..].to_string())]
    DateLit(String),

    // Identifiers
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", |lex| lex.slice().to_string(), priority = 1)]
    Ident(String),
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Let => write!(f, "let"),
            Token::From => write!(f, "from"),
            Token::Where => write!(f, "where"),
            Token::Group => write!(f, "group"),
            Token::Derive => write!(f, "derive"),
            Token::Select => write!(f, "select"),
            Token::Join => write!(f, "join"),
            Token::Window => write!(f, "window"),
            Token::Sort => write!(f, "sort"),
            Token::Take => write!(f, "take"),
            Token::Distinct => write!(f, "distinct"),
            Token::Union => write!(f, "union"),
            Token::Replicate => write!(f, "replicate"),
            Token::Check => write!(f, "check"),
            Token::Over => write!(f, "over"),
            Token::Partition => write!(f, "partition"),
            Token::Rows => write!(f, "rows"),
            Token::Range => write!(f, "range"),
            Token::Unbounded => write!(f, "unbounded"),
            Token::Current => write!(f, "current"),
            Token::Assign => write!(f, "="),
            Token::DotDot => write!(f, ".."),
            Token::Ident(s) => write!(f, "{s}"),
            Token::StringLit(s) => write!(f, "\"{s}\""),
            Token::NumberLit(s) => write!(f, "{s}"),
            Token::DateLit(s) => write!(f, "@{s}"),
            _ => write!(f, "{self:?}"),
        }
    }
}
