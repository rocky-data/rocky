//! Recursive descent parser for the Rocky DSL.

use std::sync::Arc;

use logos::Logos;

use crate::ast::*;
use crate::error::ParseError;
use crate::token::Token;

/// Parse a Rocky DSL source string into an AST.
pub fn parse(source: &str) -> Result<RockyFile, ParseError> {
    let mut parser = Parser::new(source);
    parser.parse_file()
}

struct Parser {
    tokens: Vec<(Token, usize)>,
    pos: usize,
    /// Byte offsets of each `\n` character in the source, in ascending order.
    /// Used to convert a byte offset into a (line, column) pair via binary search.
    #[allow(dead_code)] // infrastructure for upcoming line:col error reporting
    newline_offsets: Vec<usize>,
}

impl Parser {
    fn new(source: &str) -> Self {
        let mut tokens = Vec::new();
        let mut newline_offsets = Vec::new();
        let mut lexer = Token::lexer(source);
        while let Some(result) = lexer.next() {
            match result {
                Ok(token) => {
                    if token == Token::Newline {
                        newline_offsets.push(lexer.span().start);
                    } else {
                        tokens.push((token, lexer.span().start));
                    }
                }
                Err(()) => {
                    // Skip unknown tokens (whitespace, etc.)
                }
            }
        }
        Parser {
            tokens,
            pos: 0,
            newline_offsets,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|(t, _)| t)
    }

    fn advance(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let token = self.tokens[self.pos].0.clone();
            self.pos += 1;
            Some(token)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<(), ParseError> {
        match self.peek() {
            Some(t) if t == expected => {
                self.advance();
                Ok(())
            }
            Some(t) => Err(ParseError::UnexpectedToken {
                expected: format!("{expected:?}").into(),
                found: format!("{t:?}").into(),
                offset: self.current_offset(),
            }),
            None => Err(ParseError::UnexpectedEof {
                expected: format!("{expected:?}").into(),
            }),
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.advance() {
            Some(Token::Ident(s)) => Ok(s.to_string()),
            Some(t) => Err(ParseError::UnexpectedToken {
                expected: "identifier".into(),
                found: format!("{t:?}").into(),
                offset: self.current_offset(),
            }),
            None => Err(ParseError::UnexpectedEof {
                expected: "identifier".into(),
            }),
        }
    }

    fn current_offset(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map(|(_, offset)| *offset)
            .unwrap_or(0)
    }

    /// Convert a byte offset into a 1-indexed `(line, column)` pair.
    ///
    /// Uses binary search over the recorded newline positions. An offset
    /// that falls exactly on a `\n` character is reported as the last
    /// column of that line (not the first column of the next line).
    #[allow(dead_code)] // infrastructure for upcoming line:col error reporting
    fn offset_to_line_col(&self, offset: usize) -> (usize, usize) {
        // Number of newlines strictly before `offset`.
        let line_idx = self.newline_offsets.partition_point(|&nl| nl < offset);
        let line_start = if line_idx == 0 {
            0
        } else {
            self.newline_offsets[line_idx - 1] + 1
        };
        (line_idx + 1, offset - line_start + 1)
    }

    fn parse_file(&mut self) -> Result<RockyFile, ParseError> {
        // Parse let bindings before the main pipeline.
        let mut let_bindings = Vec::new();
        while self.peek() == Some(&Token::Let) {
            let_bindings.push(self.parse_let_binding()?);
        }

        // Parse the main pipeline.
        let mut pipeline = Vec::new();
        while self.peek().is_some() {
            // Skip stray pipes between steps in the main pipeline.
            if self.peek() == Some(&Token::Pipe) {
                self.advance();
                continue;
            }
            let step = self.parse_pipeline_step()?;
            pipeline.push(step);
        }

        if pipeline.is_empty() && let_bindings.is_empty() {
            return Err(ParseError::EmptyFile);
        }

        Ok(RockyFile {
            let_bindings,
            pipeline,
        })
    }

    /// Parse a let binding: `let name = pipeline_steps`.
    ///
    /// Steps are separated by `|` (inline) or newlines (multi-line).
    /// The binding ends when we reach another `let`, end of file,
    /// or a `from` keyword that starts a new pipeline context
    /// (when we already have steps in the binding).
    fn parse_let_binding(&mut self) -> Result<LetBinding, ParseError> {
        self.expect(&Token::Let)?;
        let name = self.expect_ident()?;
        self.expect(&Token::Assign)?;

        let mut steps = Vec::new();
        let mut has_from = false;

        loop {
            // Skip pipe separators.
            if self.peek() == Some(&Token::Pipe) {
                self.advance();
                continue;
            }

            match self.peek() {
                None => break,
                // Next let binding starts.
                Some(Token::Let) => break,
                // A `from` when we already have one means the main pipeline is starting.
                Some(Token::From) if has_from => break,
                _ => {}
            }

            let step = self.parse_pipeline_step()?;
            if matches!(step, PipelineStep::From(_)) {
                has_from = true;
            }
            steps.push(step);
        }

        if steps.is_empty() {
            return Err(ParseError::UnexpectedEof {
                expected: "pipeline steps for let binding".into(),
            });
        }

        Ok(LetBinding {
            name,
            pipeline: steps,
        })
    }

    /// Check if the identifier at current position is a join-type modifier.
    /// Returns the join type if `left`, `right`, `full`, or `cross` is followed by
    /// `join` or `_join`.
    fn try_parse_join_type(&mut self) -> Option<JoinType> {
        let s = match self.peek().cloned() {
            Some(Token::Ident(s)) => s,
            _ => return None,
        };

        // Combined forms: left_join, right_join, full_join, cross_join.
        let combined = match &*s {
            "left_join" => Some(JoinType::Left),
            "right_join" => Some(JoinType::Right),
            "full_join" => Some(JoinType::Full),
            "cross_join" => Some(JoinType::Cross),
            _ => None,
        };
        if let Some(jt) = combined {
            self.advance();
            return Some(jt);
        }

        // Two-word form: `left join`, `right join`, `full join`, `cross join`.
        let modifier = match &*s {
            "left" => Some(JoinType::Left),
            "right" => Some(JoinType::Right),
            "full" => Some(JoinType::Full),
            "cross" => Some(JoinType::Cross),
            _ => None,
        };
        if let Some(jt) = modifier {
            if self.tokens.get(self.pos + 1).map(|(t, _)| t) == Some(&Token::Join) {
                self.advance(); // consume the modifier ident
                self.advance(); // consume the `join` keyword
                return Some(jt);
            }
        }

        None
    }

    fn parse_pipeline_step(&mut self) -> Result<PipelineStep, ParseError> {
        // Check for join-type modifiers before the standard keyword dispatch.
        if let Some(jt) = self.try_parse_join_type() {
            return self.parse_join_with_type(jt);
        }

        match self.peek() {
            Some(Token::From) => self.parse_from(),
            Some(Token::Where) => self.parse_where(),
            Some(Token::Group) => self.parse_group(),
            Some(Token::Derive) => self.parse_derive(),
            Some(Token::Select) => self.parse_select(),
            Some(Token::Join) => self.parse_join(),
            Some(Token::Sort) => self.parse_sort(),
            Some(Token::Take) => self.parse_take(),
            Some(Token::Distinct) => {
                self.advance();
                Ok(PipelineStep::Distinct)
            }
            Some(Token::Replicate) => {
                self.advance();
                Ok(PipelineStep::Replicate)
            }
            Some(t) => Err(ParseError::UnexpectedToken {
                expected: "pipeline step (from, where, group, derive, select, join, sort, take, distinct, replicate)".into(),
                found: format!("{t:?}").into(),
                offset: self.current_offset(),
            }),
            None => Err(ParseError::UnexpectedEof {
                expected: "pipeline step".into(),
            }),
        }
    }

    fn parse_from(&mut self) -> Result<PipelineStep, ParseError> {
        self.expect(&Token::From)?;

        // Parse source: possibly dotted name
        let mut source = self.expect_ident()?;
        while self.peek() == Some(&Token::Dot) {
            self.advance();
            let part = self.expect_ident()?;
            source = format!("{source}.{part}");
        }

        // Optional alias
        let alias = if self.peek() == Some(&Token::As) {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            None
        };

        Ok(PipelineStep::From(FromClause { source, alias }))
    }

    fn parse_where(&mut self) -> Result<PipelineStep, ParseError> {
        self.expect(&Token::Where)?;
        let expr = self.parse_expr()?;
        Ok(PipelineStep::Where(expr))
    }

    fn parse_group(&mut self) -> Result<PipelineStep, ParseError> {
        self.expect(&Token::Group)?;

        // Parse grouping keys (comma-separated identifiers before the brace)
        let mut keys = Vec::new();
        loop {
            match self.peek() {
                Some(Token::LBrace) => break,
                Some(Token::Ident(_)) => {
                    keys.push(self.expect_ident()?);
                    if self.peek() == Some(&Token::Comma) {
                        self.advance();
                    }
                }
                _ => break,
            }
        }

        // Parse aggregations { name: func(col), ... }
        self.expect(&Token::LBrace)?;
        let mut aggregations = Vec::new();
        while self.peek() != Some(&Token::RBrace) {
            let name = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let expr = self.parse_expr()?;
            aggregations.push((name, expr));
            if self.peek() == Some(&Token::Comma) {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;

        Ok(PipelineStep::Group(GroupClause { keys, aggregations }))
    }

    fn parse_derive(&mut self) -> Result<PipelineStep, ParseError> {
        self.expect(&Token::Derive)?;
        self.expect(&Token::LBrace)?;

        let mut derivations = Vec::new();
        while self.peek() != Some(&Token::RBrace) {
            let name = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let expr = self.parse_expr()?;
            derivations.push((name, expr));
            if self.peek() == Some(&Token::Comma) {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;

        Ok(PipelineStep::Derive(derivations))
    }

    fn parse_select(&mut self) -> Result<PipelineStep, ParseError> {
        self.expect(&Token::Select)?;
        self.expect(&Token::LBrace)?;

        let mut items = Vec::new();
        while self.peek() != Some(&Token::RBrace) {
            match self.peek() {
                Some(Token::Star) => {
                    self.advance();
                    items.push(SelectItem::Star);
                }
                Some(Token::Ident(_)) => {
                    let name = self.expect_ident()?;
                    if self.peek() == Some(&Token::Dot) {
                        self.advance();
                        let col = self.expect_ident()?;
                        items.push(SelectItem::QualifiedColumn(name, col));
                    } else {
                        items.push(SelectItem::Column(name));
                    }
                }
                _ => break,
            }
            if self.peek() == Some(&Token::Comma) {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;

        Ok(PipelineStep::Select(items))
    }

    fn parse_join(&mut self) -> Result<PipelineStep, ParseError> {
        self.expect(&Token::Join)?;
        self.parse_join_with_type(JoinType::Inner)
    }

    /// Parse the join body after the join type has been determined.
    fn parse_join_with_type(&mut self, join_type: JoinType) -> Result<PipelineStep, ParseError> {
        let model = self.expect_ident()?;

        let alias = if self.peek() == Some(&Token::As) {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            None
        };

        // CROSS JOIN has no ON clause.
        if join_type == JoinType::Cross {
            // Optional { keep ... }
            let keep = self.parse_keep_clause()?;
            return Ok(PipelineStep::Join(JoinClause {
                join_type,
                model,
                alias,
                on: Vec::new(),
                keep,
            }));
        }

        self.expect(&Token::On)?;
        let mut on_keys = vec![self.expect_ident()?];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            on_keys.push(self.expect_ident()?);
        }

        // Optional { keep ... }
        let keep = self.parse_keep_clause()?;

        Ok(PipelineStep::Join(JoinClause {
            join_type,
            model,
            alias,
            on: on_keys,
            keep,
        }))
    }

    /// Parse an optional `{ keep col1, col2 }` clause after a join.
    fn parse_keep_clause(&mut self) -> Result<Vec<String>, ParseError> {
        if self.peek() == Some(&Token::LBrace) {
            self.advance();
            self.expect(&Token::Keep)?;
            let mut cols = Vec::new();
            loop {
                match self.peek() {
                    Some(Token::RBrace) => break,
                    Some(Token::Ident(_)) => {
                        let name = self.expect_ident()?;
                        if self.peek() == Some(&Token::Dot) {
                            self.advance();
                            let col = self.expect_ident()?;
                            cols.push(format!("{name}.{col}"));
                        } else {
                            cols.push(name);
                        }
                        if self.peek() == Some(&Token::Comma) {
                            self.advance();
                        }
                    }
                    _ => break,
                }
            }
            self.expect(&Token::RBrace)?;
            Ok(cols)
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_sort(&mut self) -> Result<PipelineStep, ParseError> {
        self.expect(&Token::Sort)?;
        let mut keys = Vec::new();
        while let Some(Token::Ident(_)) = self.peek() {
            let col = self.expect_ident()?;
            let descending = if self.peek() == Some(&Token::Desc) {
                self.advance();
                true
            } else if self.peek() == Some(&Token::Asc) {
                self.advance();
                false
            } else {
                false
            };
            keys.push(SortKey {
                column: col,
                descending,
            });
            if self.peek() == Some(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(PipelineStep::Sort(keys))
    }

    fn parse_take(&mut self) -> Result<PipelineStep, ParseError> {
        self.expect(&Token::Take)?;
        match self.advance() {
            Some(Token::NumberLit(n)) => {
                let value: u64 = n.parse().map_err(|_| ParseError::InvalidNumber {
                    value: n.to_string(),
                })?;
                Ok(PipelineStep::Take(value))
            }
            Some(t) => Err(ParseError::UnexpectedToken {
                expected: "number".into(),
                found: format!("{t:?}").into(),
                offset: self.current_offset(),
            }),
            None => Err(ParseError::UnexpectedEof {
                expected: "number".into(),
            }),
        }
    }

    // --- Window spec parsing ---

    /// Parse `(partition col1, col2, sort col3, -col4, rows unbounded..current)`.
    fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
        self.expect(&Token::LParen)?;

        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut frame = None;

        while self.peek() != Some(&Token::RParen) {
            match self.peek() {
                Some(Token::Partition) => {
                    self.advance();
                    // Parse comma-separated column names until we hit sort/rows/range/rparen
                    loop {
                        match self.peek() {
                            Some(Token::Sort) | Some(Token::Rows) | Some(Token::Range)
                            | Some(Token::RParen) => break,
                            Some(Token::Comma) => {
                                self.advance();
                            }
                            Some(Token::Ident(_)) => {
                                partition_by.push(self.expect_ident()?);
                            }
                            _ => break,
                        }
                    }
                }
                Some(Token::Sort) => {
                    self.advance();
                    // Parse sort keys: col, -col (prefix minus = DESC)
                    loop {
                        match self.peek() {
                            Some(Token::Rows) | Some(Token::Range) | Some(Token::RParen) => break,
                            Some(Token::Comma) => {
                                self.advance();
                            }
                            Some(Token::Minus) => {
                                self.advance();
                                let col = self.expect_ident()?;
                                order_by.push(SortKey {
                                    column: col,
                                    descending: true,
                                });
                            }
                            Some(Token::Ident(_)) => {
                                let col = self.expect_ident()?;
                                order_by.push(SortKey {
                                    column: col,
                                    descending: false,
                                });
                            }
                            _ => break,
                        }
                    }
                }
                Some(Token::Rows) | Some(Token::Range) => {
                    let kind = if self.peek() == Some(&Token::Rows) {
                        self.advance();
                        FrameKind::Rows
                    } else {
                        self.advance();
                        FrameKind::Range
                    };
                    let start = self.parse_frame_bound()?;
                    self.expect(&Token::DotDot)?;
                    let end = self.parse_frame_bound()?;
                    frame = Some(WindowFrame { kind, start, end });
                }
                _ => break,
            }
        }

        self.expect(&Token::RParen)?;

        Ok(WindowSpec {
            partition_by,
            order_by,
            frame,
        })
    }

    /// Parse a frame bound: `unbounded`, `current`, or a numeric offset.
    fn parse_frame_bound(&mut self) -> Result<FrameBound, ParseError> {
        match self.peek() {
            Some(Token::Unbounded) => {
                self.advance();
                Ok(FrameBound::Unbounded)
            }
            Some(Token::Current) => {
                self.advance();
                Ok(FrameBound::Current)
            }
            Some(Token::NumberLit(_)) => {
                if let Some(Token::NumberLit(n)) = self.advance() {
                    let value: i64 = n.parse().map_err(|_| ParseError::InvalidNumber {
                        value: n.to_string(),
                    })?;
                    Ok(FrameBound::Offset(value))
                } else {
                    unreachable!()
                }
            }
            Some(t) => Err(ParseError::UnexpectedToken {
                expected: "frame bound (unbounded, current, or number)".into(),
                found: format!("{t:?}").into(),
                offset: self.current_offset(),
            }),
            None => Err(ParseError::UnexpectedEof {
                expected: "frame bound".into(),
            }),
        }
    }

    // --- Match expression parsing ---

    /// Parse `match expr { op val => result, ... _ => default }`.
    fn parse_match_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::Match)?;
        let expr = self.parse_additive()?;
        self.expect(&Token::LBrace)?;

        let mut arms = Vec::new();
        while self.peek() != Some(&Token::RBrace) {
            let pattern = self.parse_match_pattern()?;
            self.expect(&Token::FatArrow)?;
            let result = self.parse_expr()?;
            arms.push(MatchArm { pattern, result });
            if self.peek() == Some(&Token::Comma) {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;

        Ok(Expr::Match {
            expr: Arc::new(expr),
            arms,
        })
    }

    /// Parse a match arm pattern: `_ | op expr`.
    ///
    /// Supported forms:
    /// - `_`         — wildcard (ELSE)
    /// - `> expr`    — comparison
    /// - `>= expr`   — comparison
    /// - `< expr`    — comparison
    /// - `<= expr`   — comparison
    /// - `== expr`   — equality comparison
    /// - `!= expr`   — inequality comparison
    /// - `expr`      — shorthand for `== expr` (implicit equality)
    fn parse_match_pattern(&mut self) -> Result<MatchPattern, ParseError> {
        // Wildcard
        if self.peek() == Some(&Token::Underscore) {
            self.advance();
            return Ok(MatchPattern::Wildcard);
        }

        // Explicit comparison operator
        let op = match self.peek() {
            Some(Token::Gt) => Some(BinOp::Gt),
            Some(Token::Gte) => Some(BinOp::Gte),
            Some(Token::Lt) => Some(BinOp::Lt),
            Some(Token::Lte) => Some(BinOp::Lte),
            Some(Token::Eq) => Some(BinOp::Eq),
            Some(Token::Neq) => Some(BinOp::Neq),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let val = self.parse_additive()?;
            Ok(MatchPattern::Comparison(op, val))
        } else {
            // Implicit equality: bare value is sugar for == value
            let val = self.parse_additive()?;
            Ok(MatchPattern::Comparison(BinOp::Eq, val))
        }
    }

    // --- Expression parsing ---

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and_expr()?;
        while self.peek() == Some(&Token::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Arc::new(left),
                op: BinOp::Or,
                right: Arc::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_comparison()?;
        while self.peek() == Some(&Token::And) {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Arc::new(left),
                op: BinOp::And,
                right: Arc::new(right),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_additive()?;

        // IS NULL / IS NOT NULL
        if self.peek() == Some(&Token::Is) {
            self.advance();
            let negated = if self.peek() == Some(&Token::Not) {
                self.advance();
                true
            } else {
                false
            };
            self.expect(&Token::Null)?;
            return Ok(Expr::IsNull {
                expr: Arc::new(left),
                negated,
            });
        }

        // Comparison operators
        let op = match self.peek() {
            Some(Token::Eq) => Some(BinOp::Eq),
            Some(Token::Neq) => Some(BinOp::Neq),
            Some(Token::Lt) => Some(BinOp::Lt),
            Some(Token::Lte) => Some(BinOp::Lte),
            Some(Token::Gt) => Some(BinOp::Gt),
            Some(Token::Gte) => Some(BinOp::Gte),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive()?;
            left = Expr::BinaryOp {
                left: Arc::new(left),
                op,
                right: Arc::new(right),
            };
        }

        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => BinOp::Add,
                Some(Token::Minus) => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinaryOp {
                left: Arc::new(left),
                op,
                right: Arc::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => BinOp::Mul,
                Some(Token::Slash) => BinOp::Div,
                Some(Token::Percent) => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp {
                left: Arc::new(left),
                op,
                right: Arc::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        match self.peek() {
            Some(Token::Not) => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Arc::new(expr),
                })
            }
            Some(Token::Minus) => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Neg,
                    expr: Arc::new(expr),
                })
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().cloned() {
            Some(Token::StringLit(_)) => {
                if let Some(Token::StringLit(s)) = self.advance() {
                    Ok(Expr::StringLit(s.to_string()))
                } else {
                    unreachable!()
                }
            }
            Some(Token::NumberLit(_)) => {
                if let Some(Token::NumberLit(n)) = self.advance() {
                    Ok(Expr::NumberLit(n.to_string()))
                } else {
                    unreachable!()
                }
            }
            Some(Token::DateLit(_)) => {
                if let Some(Token::DateLit(d)) = self.advance() {
                    Ok(Expr::DateLit(d.to_string()))
                } else {
                    unreachable!()
                }
            }
            Some(Token::True) => {
                self.advance();
                Ok(Expr::BoolLit(true))
            }
            Some(Token::False) => {
                self.advance();
                Ok(Expr::BoolLit(false))
            }
            Some(Token::Null) => {
                self.advance();
                Ok(Expr::Null)
            }
            Some(Token::Match) => self.parse_match_expr(),
            Some(Token::LParen) => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(Token::Ident(_)) => {
                let name = self.expect_ident()?;

                // Function call: name(args)
                if self.peek() == Some(&Token::LParen) {
                    self.advance();
                    let mut args = Vec::new();
                    while self.peek() != Some(&Token::RParen) {
                        args.push(self.parse_expr()?);
                        if self.peek() == Some(&Token::Comma) {
                            self.advance();
                        }
                    }
                    self.expect(&Token::RParen)?;

                    // Window function: name(args) over (...)
                    if self.peek() == Some(&Token::Over) {
                        self.advance();
                        let over = self.parse_window_spec()?;
                        return Ok(Expr::WindowFunction { name, args, over });
                    }

                    return Ok(Expr::FunctionCall { name, args });
                }

                // Qualified column: alias.column
                if self.peek() == Some(&Token::Dot) {
                    self.advance();
                    let col = self.expect_ident()?;
                    return Ok(Expr::QualifiedColumn(name, col));
                }

                Ok(Expr::Column(name))
            }
            Some(t) => Err(ParseError::UnexpectedToken {
                expected: "expression".into(),
                found: format!("{t:?}").into(),
                offset: self.current_offset(),
            }),
            None => Err(ParseError::UnexpectedEof {
                expected: "expression".into(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_pipeline() {
        let input = r#"
            from orders
            where status == "completed"
            select { id, amount }
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.pipeline.len(), 3);
        assert!(matches!(file.pipeline[0], PipelineStep::From(_)));
        assert!(matches!(file.pipeline[1], PipelineStep::Where(_)));
        assert!(matches!(file.pipeline[2], PipelineStep::Select(_)));
    }

    #[test]
    fn test_parse_from_with_alias() {
        let input = "from orders as o";
        let file = parse(input).unwrap();
        if let PipelineStep::From(from) = &file.pipeline[0] {
            assert_eq!(from.source, "orders");
            assert_eq!(from.alias, Some("o".to_string()));
        } else {
            panic!("expected From");
        }
    }

    #[test]
    fn test_parse_from_dotted() {
        let input = "from source.fivetran.orders";
        let file = parse(input).unwrap();
        if let PipelineStep::From(from) = &file.pipeline[0] {
            assert_eq!(from.source, "source.fivetran.orders");
        } else {
            panic!("expected From");
        }
    }

    #[test]
    fn test_parse_derive() {
        let input = r#"
            from orders
            derive {
                total: amount * quantity,
                label: "fixed"
            }
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.pipeline.len(), 2);
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            assert_eq!(derivations.len(), 2);
            assert_eq!(derivations[0].0, "total");
            assert_eq!(derivations[1].0, "label");
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_group() {
        let input = r#"
            from orders
            group customer_id {
                total_revenue: sum(amount),
                order_count: count()
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Group(group) = &file.pipeline[1] {
            assert_eq!(group.keys, vec!["customer_id"]);
            assert_eq!(group.aggregations.len(), 2);
        } else {
            panic!("expected Group");
        }
    }

    #[test]
    fn test_parse_join() {
        let input = r#"
            from orders as o
            join customers as c on customer_id {
                keep c.name, c.email
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.model, "customers");
            assert_eq!(join.alias, Some("c".to_string()));
            assert_eq!(join.on, vec!["customer_id"]);
            assert_eq!(join.keep.len(), 2);
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_sort_take() {
        let input = r#"
            from orders
            sort amount desc, name
            take 100
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Sort(keys) = &file.pipeline[1] {
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0].column, "amount");
            assert!(keys[0].descending);
            assert_eq!(keys[1].column, "name");
            assert!(!keys[1].descending);
        } else {
            panic!("expected Sort");
        }
        assert!(matches!(file.pipeline[2], PipelineStep::Take(100)));
    }

    #[test]
    fn test_parse_replicate() {
        let input = "from source.fivetran.orders\nreplicate";
        let file = parse(input).unwrap();
        assert!(matches!(file.pipeline[1], PipelineStep::Replicate));
    }

    #[test]
    fn test_parse_where_comparison() {
        let input = r#"
            from orders
            where amount >= 100 and status != "cancelled"
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Where(Expr::BinaryOp { op: BinOp::And, .. }) = &file.pipeline[1] {
            // ok — parsed as AND of two comparisons
        } else {
            panic!("expected Where with AND");
        }
    }

    #[test]
    fn test_parse_date_literal() {
        let input = r#"
            from orders
            where order_date >= @2025-01-01
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Where(Expr::BinaryOp { right, .. }) = &file.pipeline[1] {
            assert!(matches!(right.as_ref(), Expr::DateLit(d) if d == "2025-01-01"));
        } else {
            panic!("expected date comparison");
        }
    }

    #[test]
    fn test_parse_function_call() {
        let input = r#"
            from orders
            group customer_id {
                total: sum(amount)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Group(group) = &file.pipeline[1] {
            if let Expr::FunctionCall { name, args } = &group.aggregations[0].1 {
                assert_eq!(name, "sum");
                assert_eq!(args.len(), 1);
            } else {
                panic!("expected function call");
            }
        }
    }

    #[test]
    fn test_parse_distinct() {
        let input = "from orders\ndistinct";
        let file = parse(input).unwrap();
        assert!(matches!(file.pipeline[1], PipelineStep::Distinct));
    }

    #[test]
    fn test_parse_is_null() {
        let input = "from orders\nwhere email is not null";
        let file = parse(input).unwrap();
        if let PipelineStep::Where(Expr::IsNull { negated, .. }) = &file.pipeline[1] {
            assert!(negated);
        } else {
            panic!("expected IS NOT NULL");
        }
    }

    #[test]
    fn test_empty_file_error() {
        assert!(parse("").is_err());
    }

    #[test]
    fn test_comment_ignored() {
        let input = "-- this is a comment\nfrom orders";
        let file = parse(input).unwrap();
        assert_eq!(file.pipeline.len(), 1);
    }

    #[test]
    fn test_parse_window_row_number() {
        let input = r#"
            from orders
            derive {
                rn: row_number() over (partition customer_id, sort -order_date)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            assert_eq!(derivations[0].0, "rn");
            if let Expr::WindowFunction { name, args, over } = &derivations[0].1 {
                assert_eq!(name, "row_number");
                assert!(args.is_empty());
                assert_eq!(over.partition_by, vec!["customer_id"]);
                assert_eq!(over.order_by.len(), 1);
                assert_eq!(over.order_by[0].column, "order_date");
                assert!(over.order_by[0].descending);
                assert!(over.frame.is_none());
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_sum_with_frame() {
        let input = r#"
            from orders
            derive {
                running_total: sum(amount) over (partition customer_id, sort order_date, rows unbounded..current)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, args, over } = &derivations[0].1 {
                assert_eq!(name, "sum");
                assert_eq!(args.len(), 1);
                assert_eq!(over.partition_by, vec!["customer_id"]);
                assert_eq!(over.order_by.len(), 1);
                assert_eq!(over.order_by[0].column, "order_date");
                assert!(!over.order_by[0].descending);
                let frame = over.frame.as_ref().unwrap();
                assert!(matches!(frame.kind, FrameKind::Rows));
                assert!(matches!(frame.start, FrameBound::Unbounded));
                assert!(matches!(frame.end, FrameBound::Current));
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_lag_no_partition() {
        let input = r#"
            from orders
            derive {
                prev_amount: lag(amount, 1) over (sort order_date)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, args, over } = &derivations[0].1 {
                assert_eq!(name, "lag");
                assert_eq!(args.len(), 2);
                assert!(over.partition_by.is_empty());
                assert_eq!(over.order_by.len(), 1);
                assert_eq!(over.order_by[0].column, "order_date");
                assert!(!over.order_by[0].descending);
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_rank() {
        let input = r#"
            from orders
            derive {
                rnk: rank() over (partition region, sort -revenue)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, args, over } = &derivations[0].1 {
                assert_eq!(name, "rank");
                assert!(args.is_empty());
                assert_eq!(over.partition_by, vec!["region"]);
                assert_eq!(over.order_by.len(), 1);
                assert_eq!(over.order_by[0].column, "revenue");
                assert!(over.order_by[0].descending);
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_dense_rank() {
        let input = r#"
            from orders
            derive {
                dr: dense_rank() over (partition category, sort -sales)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, args, over } = &derivations[0].1 {
                assert_eq!(name, "dense_rank");
                assert!(args.is_empty());
                assert_eq!(over.partition_by, vec!["category"]);
                assert_eq!(over.order_by[0].column, "sales");
                assert!(over.order_by[0].descending);
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_lead() {
        let input = r#"
            from orders
            derive {
                next_amount: lead(amount, 1) over (partition customer_id, sort order_date)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, args, over } = &derivations[0].1 {
                assert_eq!(name, "lead");
                assert_eq!(args.len(), 2);
                assert_eq!(over.partition_by, vec!["customer_id"]);
                assert_eq!(over.order_by[0].column, "order_date");
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_avg() {
        let input = r#"
            from orders
            derive {
                avg_amount: avg(amount) over (partition region)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, args, over } = &derivations[0].1 {
                assert_eq!(name, "avg");
                assert_eq!(args.len(), 1);
                assert_eq!(over.partition_by, vec!["region"]);
                assert!(over.order_by.is_empty());
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_count() {
        let input = r#"
            from orders
            derive {
                cnt: count() over (partition customer_id)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, args, over } = &derivations[0].1 {
                assert_eq!(name, "count");
                assert!(args.is_empty());
                assert_eq!(over.partition_by, vec!["customer_id"]);
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_min_max() {
        let input = r#"
            from orders
            derive {
                min_amt: min(amount) over (partition region),
                max_amt: max(amount) over (partition region)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            assert_eq!(derivations.len(), 2);
            if let Expr::WindowFunction { name, .. } = &derivations[0].1 {
                assert_eq!(name, "min");
            } else {
                panic!("expected WindowFunction for min");
            }
            if let Expr::WindowFunction { name, .. } = &derivations[1].1 {
                assert_eq!(name, "max");
            } else {
                panic!("expected WindowFunction for max");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_multiple_partition_keys() {
        let input = r#"
            from orders
            derive {
                rn: row_number() over (partition region, category, year, sort -revenue)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { over, .. } = &derivations[0].1 {
                assert_eq!(over.partition_by, vec!["region", "category", "year"]);
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_multiple_sort_keys() {
        let input = r#"
            from orders
            derive {
                rn: row_number() over (sort -revenue, order_date)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { over, .. } = &derivations[0].1 {
                assert_eq!(over.order_by.len(), 2);
                assert_eq!(over.order_by[0].column, "revenue");
                assert!(over.order_by[0].descending);
                assert_eq!(over.order_by[1].column, "order_date");
                assert!(!over.order_by[1].descending);
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_range_frame() {
        let input = r#"
            from orders
            derive {
                total: sum(amount) over (sort order_date, range unbounded..current)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { over, .. } = &derivations[0].1 {
                let frame = over.frame.as_ref().unwrap();
                assert!(matches!(frame.kind, FrameKind::Range));
                assert!(matches!(frame.start, FrameBound::Unbounded));
                assert!(matches!(frame.end, FrameBound::Current));
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_offset_frame() {
        let input = r#"
            from orders
            derive {
                moving_avg: avg(amount) over (sort order_date, rows 3..current)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { over, .. } = &derivations[0].1 {
                let frame = over.frame.as_ref().unwrap();
                assert!(matches!(frame.kind, FrameKind::Rows));
                assert!(matches!(frame.start, FrameBound::Offset(3)));
                assert!(matches!(frame.end, FrameBound::Current));
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_lead_with_default() {
        let input = r#"
            from orders
            derive {
                next_val: lead(amount, 1, 0) over (partition customer_id, sort order_date)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, args, .. } = &derivations[0].1 {
                assert_eq!(name, "lead");
                assert_eq!(args.len(), 3);
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_window_no_partition_no_order() {
        let input = r#"
            from orders
            derive {
                total_count: count() over ()
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::WindowFunction { name, over, .. } = &derivations[0].1 {
                assert_eq!(name, "count");
                assert!(over.partition_by.is_empty());
                assert!(over.order_by.is_empty());
                assert!(over.frame.is_none());
            } else {
                panic!("expected WindowFunction");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_multiple_window_functions() {
        let input = r#"
            from orders
            derive {
                rn: row_number() over (partition customer_id, sort -order_date),
                rnk: rank() over (partition customer_id, sort -amount),
                running: sum(amount) over (partition customer_id, sort order_date, rows unbounded..current)
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            assert_eq!(derivations.len(), 3);
            assert!(
                matches!(&derivations[0].1, Expr::WindowFunction { name, .. } if name == "row_number")
            );
            assert!(
                matches!(&derivations[1].1, Expr::WindowFunction { name, .. } if name == "rank")
            );
            assert!(
                matches!(&derivations[2].1, Expr::WindowFunction { name, .. } if name == "sum")
            );
        } else {
            panic!("expected Derive");
        }
    }

    // --- Match expression parsing tests ---

    #[test]
    fn test_parse_match_comparison_arms() {
        let input = r#"
            from orders
            derive {
                tier: match amount {
                    > 10000 => "high",
                    > 5000 => "medium",
                    _ => "low"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            assert_eq!(derivations[0].0, "tier");
            if let Expr::Match { expr, arms } = &derivations[0].1 {
                assert!(matches!(expr.as_ref(), Expr::Column(c) if c == "amount"));
                assert_eq!(arms.len(), 3);
                assert!(matches!(
                    &arms[0].pattern,
                    MatchPattern::Comparison(BinOp::Gt, _)
                ));
                assert!(matches!(
                    &arms[1].pattern,
                    MatchPattern::Comparison(BinOp::Gt, _)
                ));
                assert!(matches!(&arms[2].pattern, MatchPattern::Wildcard));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_equality_explicit() {
        let input = r#"
            from orders
            derive {
                label: match status {
                    == "active" => "Active",
                    == "pending" => "Pending",
                    _ => "Other"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 3);
                assert!(
                    matches!(&arms[0].pattern, MatchPattern::Comparison(BinOp::Eq, Expr::StringLit(s)) if s == "active")
                );
                assert!(
                    matches!(&arms[1].pattern, MatchPattern::Comparison(BinOp::Eq, Expr::StringLit(s)) if s == "pending")
                );
                assert!(matches!(&arms[2].pattern, MatchPattern::Wildcard));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_implicit_equality() {
        let input = r#"
            from orders
            derive {
                label: match status {
                    "active" => "Active",
                    "pending" => "Pending",
                    _ => "Other"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 3);
                // Implicit equality: bare string parses as Comparison(Eq, ...)
                assert!(
                    matches!(&arms[0].pattern, MatchPattern::Comparison(BinOp::Eq, Expr::StringLit(s)) if s == "active")
                );
                assert!(
                    matches!(&arms[1].pattern, MatchPattern::Comparison(BinOp::Eq, Expr::StringLit(s)) if s == "pending")
                );
                assert!(matches!(&arms[2].pattern, MatchPattern::Wildcard));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_numeric_patterns() {
        let input = r#"
            from orders
            derive {
                bucket: match score {
                    >= 90 => "A",
                    >= 80 => "B",
                    >= 70 => "C",
                    _ => "F"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 4);
                assert!(
                    matches!(&arms[0].pattern, MatchPattern::Comparison(BinOp::Gte, Expr::NumberLit(n)) if n == "90")
                );
                assert!(
                    matches!(&arms[1].pattern, MatchPattern::Comparison(BinOp::Gte, Expr::NumberLit(n)) if n == "80")
                );
                assert!(
                    matches!(&arms[2].pattern, MatchPattern::Comparison(BinOp::Gte, Expr::NumberLit(n)) if n == "70")
                );
                assert!(matches!(&arms[3].pattern, MatchPattern::Wildcard));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_no_wildcard() {
        let input = r#"
            from orders
            derive {
                flag: match priority {
                    > 5 => true,
                    <= 5 => false
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 2);
                // No wildcard arm
                assert!(matches!(
                    &arms[0].pattern,
                    MatchPattern::Comparison(BinOp::Gt, _)
                ));
                assert!(matches!(
                    &arms[1].pattern,
                    MatchPattern::Comparison(BinOp::Lte, _)
                ));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_with_neq() {
        let input = r#"
            from orders
            derive {
                label: match status {
                    != "cancelled" => "valid",
                    _ => "cancelled"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 2);
                assert!(matches!(
                    &arms[0].pattern,
                    MatchPattern::Comparison(BinOp::Neq, _)
                ));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_expression_on_arithmetic() {
        let input = r#"
            from orders
            derive {
                tier: match amount * quantity {
                    > 1000 => "big",
                    _ => "small"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { expr, arms } = &derivations[0].1 {
                // The match expression should be a BinaryOp (amount * quantity)
                assert!(matches!(
                    expr.as_ref(),
                    Expr::BinaryOp { op: BinOp::Mul, .. }
                ));
                assert_eq!(arms.len(), 2);
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_result_with_function_call() {
        let input = r#"
            from orders
            derive {
                cleaned: match status {
                    == "ACTIVE" => "active",
                    _ => lower(status)
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 2);
                assert!(matches!(&arms[0].result, Expr::StringLit(s) if s == "active"));
                assert!(
                    matches!(&arms[1].result, Expr::FunctionCall { name, .. } if name == "lower")
                );
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_single_arm() {
        let input = r#"
            from orders
            derive {
                label: match active {
                    _ => "default"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 1);
                assert!(matches!(&arms[0].pattern, MatchPattern::Wildcard));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_result_is_null() {
        let input = r#"
            from orders
            derive {
                val: match status {
                    == "unknown" => null,
                    _ => status
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert!(matches!(&arms[0].result, Expr::Null));
                assert!(matches!(&arms[1].result, Expr::Column(c) if c == "status"));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_result_is_numeric() {
        let input = r#"
            from orders
            derive {
                score: match grade {
                    == "A" => 4.0,
                    == "B" => 3.0,
                    == "C" => 2.0,
                    _ => 0
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 4);
                assert!(matches!(&arms[0].result, Expr::NumberLit(n) if n == "4.0"));
                assert!(matches!(&arms[3].result, Expr::NumberLit(n) if n == "0"));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_mixed_operators() {
        let input = r#"
            from orders
            derive {
                range_label: match value {
                    < 0 => "negative",
                    == 0 => "zero",
                    <= 100 => "small",
                    <= 1000 => "medium",
                    _ => "large"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 5);
                assert!(matches!(
                    &arms[0].pattern,
                    MatchPattern::Comparison(BinOp::Lt, _)
                ));
                assert!(matches!(
                    &arms[1].pattern,
                    MatchPattern::Comparison(BinOp::Eq, _)
                ));
                assert!(matches!(
                    &arms[2].pattern,
                    MatchPattern::Comparison(BinOp::Lte, _)
                ));
                assert!(matches!(
                    &arms[3].pattern,
                    MatchPattern::Comparison(BinOp::Lte, _)
                ));
                assert!(matches!(&arms[4].pattern, MatchPattern::Wildcard));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_with_date_pattern() {
        let input = r#"
            from orders
            derive {
                period: match order_date {
                    >= @2025-01-01 => "current",
                    _ => "historical"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 2);
                assert!(
                    matches!(&arms[0].pattern, MatchPattern::Comparison(BinOp::Gte, Expr::DateLit(d)) if d == "2025-01-01")
                );
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_trailing_comma() {
        let input = r#"
            from orders
            derive {
                tier: match amount {
                    > 10000 => "high",
                    _ => "low",
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 2);
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_implicit_eq_with_numbers() {
        let input = r#"
            from orders
            derive {
                label: match priority {
                    1 => "urgent",
                    2 => "high",
                    3 => "normal",
                    _ => "low"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 4);
                assert!(
                    matches!(&arms[0].pattern, MatchPattern::Comparison(BinOp::Eq, Expr::NumberLit(n)) if n == "1")
                );
                assert!(
                    matches!(&arms[1].pattern, MatchPattern::Comparison(BinOp::Eq, Expr::NumberLit(n)) if n == "2")
                );
                assert!(
                    matches!(&arms[2].pattern, MatchPattern::Comparison(BinOp::Eq, Expr::NumberLit(n)) if n == "3")
                );
                assert!(matches!(&arms[3].pattern, MatchPattern::Wildcard));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_implicit_eq_with_booleans() {
        let input = r#"
            from orders
            derive {
                label: match active {
                    true => "active",
                    false => "inactive"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 2);
                assert!(matches!(
                    &arms[0].pattern,
                    MatchPattern::Comparison(BinOp::Eq, Expr::BoolLit(true))
                ));
                assert!(matches!(
                    &arms[1].pattern,
                    MatchPattern::Comparison(BinOp::Eq, Expr::BoolLit(false))
                ));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_on_qualified_column() {
        let input = r#"
            from orders as o
            derive {
                tier: match o.amount {
                    > 10000 => "high",
                    _ => "low"
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { expr, .. } = &derivations[0].1 {
                assert!(
                    matches!(expr.as_ref(), Expr::QualifiedColumn(a, c) if a == "o" && c == "amount")
                );
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    #[test]
    fn test_parse_match_result_arithmetic() {
        let input = r#"
            from orders
            derive {
                adjusted: match region {
                    == "EU" => amount * 1.2,
                    == "APAC" => amount * 1.1,
                    _ => amount
                }
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Derive(derivations) = &file.pipeline[1] {
            if let Expr::Match { arms, .. } = &derivations[0].1 {
                assert_eq!(arms.len(), 3);
                assert!(matches!(
                    &arms[0].result,
                    Expr::BinaryOp { op: BinOp::Mul, .. }
                ));
            } else {
                panic!("expected Match expression");
            }
        } else {
            panic!("expected Derive");
        }
    }

    // ==========================================
    // Plan 12: Let bindings (CTE sub-pipelines)
    // ==========================================

    #[test]
    fn test_parse_single_let_binding() {
        let input = r#"
            let active = from users
                where is_active == true
            from active
            select { id, name }
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        assert_eq!(file.let_bindings[0].name, "active");
        assert_eq!(file.let_bindings[0].pipeline.len(), 2);
        assert!(matches!(
            file.let_bindings[0].pipeline[0],
            PipelineStep::From(_)
        ));
        assert!(matches!(
            file.let_bindings[0].pipeline[1],
            PipelineStep::Where(_)
        ));
        assert_eq!(file.pipeline.len(), 2);
    }

    #[test]
    fn test_parse_let_binding_inline_pipe() {
        let input =
            "let active = from users | where is_active == true\nfrom active | select { id }";
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        assert_eq!(file.let_bindings[0].name, "active");
        assert_eq!(file.let_bindings[0].pipeline.len(), 2);
        assert_eq!(file.pipeline.len(), 2);
    }

    #[test]
    fn test_parse_multiple_let_bindings() {
        let input = r#"
            let active = from users
                where is_active == true
            let recent = from orders
                where order_date >= @2025-01-01
            from active
            join recent on user_id
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 2);
        assert_eq!(file.let_bindings[0].name, "active");
        assert_eq!(file.let_bindings[1].name, "recent");
        assert_eq!(file.pipeline.len(), 2);
    }

    #[test]
    fn test_parse_let_binding_with_group() {
        let input = r#"
            let totals = from orders
                group customer_id {
                    total: sum(amount)
                }
            from totals
            where total > 100
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        assert_eq!(file.let_bindings[0].pipeline.len(), 2);
        assert!(matches!(
            file.let_bindings[0].pipeline[1],
            PipelineStep::Group(_)
        ));
    }

    #[test]
    fn test_parse_let_binding_with_derive() {
        let input = r#"
            let enriched = from orders
                derive {
                    total: amount * quantity
                }
            from enriched
            where total > 50
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        assert_eq!(file.let_bindings[0].pipeline.len(), 2);
        assert!(matches!(
            file.let_bindings[0].pipeline[1],
            PipelineStep::Derive(_)
        ));
    }

    #[test]
    fn test_parse_let_binding_with_sort_take() {
        let input = r#"
            let top_orders = from orders
                sort amount desc
                take 100
            from top_orders
            select { id, amount }
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        assert_eq!(file.let_bindings[0].pipeline.len(), 3);
    }

    #[test]
    fn test_parse_let_binding_with_distinct() {
        let input = r#"
            let unique_customers = from orders
                select { customer_id }
                distinct
            from unique_customers
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        assert_eq!(file.let_bindings[0].pipeline.len(), 3);
        assert!(matches!(
            file.let_bindings[0].pipeline[2],
            PipelineStep::Distinct
        ));
    }

    #[test]
    fn test_parse_let_no_bindings_backward_compat() {
        let input = "from orders\nselect { id }";
        let file = parse(input).unwrap();
        assert!(file.let_bindings.is_empty());
        assert_eq!(file.pipeline.len(), 2);
    }

    #[test]
    fn test_parse_let_empty_binding_error() {
        let result = parse("let empty =");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_three_let_bindings() {
        let input = r#"
            let a = from t1
                where x == 1
            let b = from t2
                where y == 2
            let c = from t3
                where z == 3
            from a
            join b on id
            join c on id
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 3);
        assert_eq!(file.let_bindings[0].name, "a");
        assert_eq!(file.let_bindings[1].name, "b");
        assert_eq!(file.let_bindings[2].name, "c");
        assert_eq!(file.pipeline.len(), 3);
    }

    #[test]
    fn test_parse_let_binding_with_window() {
        let input = r#"
            let ranked = from orders
                derive {
                    rn: row_number() over (partition customer_id, sort -amount)
                }
            from ranked
            where rn == 1
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        if let PipelineStep::Derive(derivations) = &file.let_bindings[0].pipeline[1] {
            assert!(matches!(&derivations[0].1, Expr::WindowFunction { .. }));
        } else {
            panic!("expected Derive with window function");
        }
    }

    // ==========================================
    // Plan 13: Complex join types
    // ==========================================

    #[test]
    fn test_parse_inner_join_default() {
        let input = r#"
            from orders as o
            join customers as c on customer_id
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Inner);
            assert_eq!(join.model, "customers");
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_left_join_underscore() {
        let input = r#"
            from orders as o
            left_join customers as c on customer_id
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Left);
            assert_eq!(join.model, "customers");
            assert_eq!(join.alias, Some("c".to_string()));
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_left_join_two_word() {
        let input = r#"
            from orders as o
            left join customers as c on customer_id
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Left);
            assert_eq!(join.model, "customers");
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_right_join_underscore() {
        let input = r#"
            from orders as o
            right_join customers as c on customer_id
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Right);
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_right_join_two_word() {
        let input = r#"
            from orders as o
            right join customers as c on customer_id
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Right);
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_full_join_underscore() {
        let input = r#"
            from orders as o
            full_join customers as c on customer_id
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Full);
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_full_join_two_word() {
        let input = r#"
            from orders as o
            full join customers as c on customer_id
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Full);
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_cross_join_underscore() {
        let input = "from orders\ncross_join dates";
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Cross);
            assert_eq!(join.model, "dates");
            assert!(join.on.is_empty());
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_cross_join_two_word() {
        let input = "from orders\ncross join dates";
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Cross);
            assert!(join.on.is_empty());
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_left_join_with_keep() {
        let input = r#"
            from orders as o
            left_join customers as c on customer_id {
                keep c.name, c.email
            }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Left);
            assert_eq!(join.keep.len(), 2);
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_cross_join_with_alias() {
        let input = "from orders as o\ncross_join dates as d";
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Cross);
            assert_eq!(join.alias, Some("d".to_string()));
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn test_parse_multiple_join_types() {
        let input = r#"
            from orders as o
            join customers as c on customer_id
            left_join products as p on product_id
            cross_join dates as d
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.pipeline.len(), 4);
        if let PipelineStep::Join(j) = &file.pipeline[1] {
            assert_eq!(j.join_type, JoinType::Inner);
        } else {
            panic!("expected inner join");
        }
        if let PipelineStep::Join(j) = &file.pipeline[2] {
            assert_eq!(j.join_type, JoinType::Left);
        } else {
            panic!("expected left join");
        }
        if let PipelineStep::Join(j) = &file.pipeline[3] {
            assert_eq!(j.join_type, JoinType::Cross);
        } else {
            panic!("expected cross join");
        }
    }

    #[test]
    fn test_parse_left_as_column_name() {
        // `left` should still work as a column name when not followed by `join`.
        let input = r#"
            from orders
            select { left, right }
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Select(items) = &file.pipeline[1] {
            assert_eq!(items.len(), 2);
            assert!(matches!(&items[0], SelectItem::Column(name) if name == "left"));
            assert!(matches!(&items[1], SelectItem::Column(name) if name == "right"));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_parse_left_join_multiple_keys() {
        let input = r#"
            from orders as o
            left_join products as p on product_id, variant_id
        "#;
        let file = parse(input).unwrap();
        if let PipelineStep::Join(join) = &file.pipeline[1] {
            assert_eq!(join.join_type, JoinType::Left);
            assert_eq!(join.on, vec!["product_id", "variant_id"]);
        } else {
            panic!("expected Join");
        }
    }

    // ==========================================
    // Combined: Let bindings + Complex joins
    // ==========================================

    #[test]
    fn test_parse_let_with_left_join() {
        let input = r#"
            let enriched = from orders as o
                left_join customers as c on customer_id {
                    keep c.name
                }
            from enriched
            where name is not null
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        assert_eq!(file.let_bindings[0].pipeline.len(), 2);
        if let PipelineStep::Join(join) = &file.let_bindings[0].pipeline[1] {
            assert_eq!(join.join_type, JoinType::Left);
        } else {
            panic!("expected left join in let binding");
        }
    }

    #[test]
    fn test_parse_let_cross_join_in_binding() {
        let input = r#"
            let expanded = from orders
                cross_join dates
            from expanded
            select { order_id, date }
        "#;
        let file = parse(input).unwrap();
        assert_eq!(file.let_bindings.len(), 1);
        if let PipelineStep::Join(join) = &file.let_bindings[0].pipeline[1] {
            assert_eq!(join.join_type, JoinType::Cross);
        } else {
            panic!("expected cross join in let binding");
        }
    }

    // --- Newline tracking / offset_to_line_col tests ---

    #[test]
    fn test_offset_to_line_col_offset_zero() {
        let parser = Parser::new("from orders");
        assert_eq!(parser.offset_to_line_col(0), (1, 1));
    }

    #[test]
    fn test_offset_to_line_col_no_newlines() {
        let parser = Parser::new("from orders");
        // "orders" starts at byte 5
        assert_eq!(parser.offset_to_line_col(5), (1, 6));
    }

    #[test]
    fn test_offset_to_line_col_multiline() {
        // "from orders\nwhere x == 1"
        //  0123456789 10 11 = \n, 12 = 'w'
        let input = "from orders\nwhere x == 1";
        let parser = Parser::new(input);
        // Newline at byte 11
        assert_eq!(parser.newline_offsets, vec![11]);
        // 'f' at offset 0 → line 1, col 1
        assert_eq!(parser.offset_to_line_col(0), (1, 1));
        // 'o' at offset 5 → line 1, col 6
        assert_eq!(parser.offset_to_line_col(5), (1, 6));
        // '\n' at offset 11 → line 1, col 12 (last col of line 1)
        assert_eq!(parser.offset_to_line_col(11), (1, 12));
        // 'w' at offset 12 → line 2, col 1
        assert_eq!(parser.offset_to_line_col(12), (2, 1));
        // 'x' at offset 18 → line 2, col 7
        assert_eq!(parser.offset_to_line_col(18), (2, 7));
    }

    #[test]
    fn test_offset_to_line_col_three_lines() {
        // "a\nb\nc"
        //  0  1=\n  2  3=\n  4
        let input = "a\nb\nc";
        let parser = Parser::new(input);
        assert_eq!(parser.newline_offsets, vec![1, 3]);
        assert_eq!(parser.offset_to_line_col(0), (1, 1)); // 'a'
        assert_eq!(parser.offset_to_line_col(1), (1, 2)); // '\n' → last col of line 1
        assert_eq!(parser.offset_to_line_col(2), (2, 1)); // 'b'
        assert_eq!(parser.offset_to_line_col(3), (2, 2)); // '\n' → last col of line 2
        assert_eq!(parser.offset_to_line_col(4), (3, 1)); // 'c' → line 3, col 1
    }

    #[test]
    fn test_offset_to_line_col_empty_input() {
        let parser = Parser::new("");
        // No newlines recorded
        assert!(parser.newline_offsets.is_empty());
        assert_eq!(parser.offset_to_line_col(0), (1, 1));
    }

    #[test]
    fn test_offset_to_line_col_offset_in_last_line() {
        let input = "from orders\nwhere x == 1\nselect { id }";
        let parser = Parser::new(input);
        // Newlines at 11 and 24
        assert_eq!(parser.newline_offsets.len(), 2);
        // "select" starts at byte 25 → line 3, col 1
        assert_eq!(parser.offset_to_line_col(25), (3, 1));
        // "id" is deeper in line 3
        let id_offset = input.rfind("id").unwrap();
        assert_eq!(parser.offset_to_line_col(id_offset), (3, id_offset - 24));
    }

    #[test]
    fn test_newline_offsets_collected() {
        let input = "from orders\nwhere status == \"completed\"\nselect { id }";
        let parser = Parser::new(input);
        assert_eq!(parser.newline_offsets.len(), 2);
        assert_eq!(parser.newline_offsets[0], 11);
        // Second newline is after "completed\"
        let second_nl = input.find('\n').unwrap();
        let second_nl2 = input[second_nl + 1..].find('\n').unwrap() + second_nl + 1;
        assert_eq!(parser.newline_offsets[1], second_nl2);
    }
}
