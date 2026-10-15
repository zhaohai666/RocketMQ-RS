use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::common::{Result, invalid};

/// Values reachable from the original grammar. Java's Byte/Short/Float/BigDecimal
/// branches cannot be produced by its literals, so they are absent here.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Str(String),
}

impl Value {
    fn is_number(&self) -> bool {
        matches!(self, Self::I32(_) | Self::I64(_) | Self::F64(_))
    }

    /// The original engine reports `ConstantExpression.toString()` in filter errors.
    fn describe(&self) -> String {
        match self {
            Self::Null => "NULL".into(),
            Self::Bool(true) => "TRUE".into(),
            Self::Bool(false) => "FALSE".into(),
            Self::I32(value) => value.to_string(),
            Self::I64(value) => value.to_string(),
            Self::F64(value) => value.to_string(),
            Self::Str(text) => format!("'{}'", text.replace('\'', "''")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Order {
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Affix {
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Clone, Debug, PartialEq)]
enum Node {
    Const(Value),
    Property(String),
    Ordered(Order, Box<Node>, Box<Node>),
    Equal(Box<Node>, Box<Node>),
    Not(Box<Node>),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    BooleanCast(Box<Node>),
    Negate(Box<Node>),
    In(Box<Node>, Vec<String>, bool),
    Text(Affix, Box<Node>, String, bool),
}

/// Which Java classes implement `BooleanExpression`; `asBooleanExpression` depends on this.
fn is_boolean(node: &Node) -> bool {
    match node {
        // TRUE, FALSE and NULL literals are BooleanConstantExpression in the original parser.
        Node::Const(Value::Bool(_) | Value::Null) => true,
        Node::Ordered(..)
        | Node::Equal(..)
        | Node::Not(..)
        | Node::And(..)
        | Node::Or(..)
        | Node::BooleanCast(..)
        | Node::In(..)
        | Node::Text(..) => true,
        Node::Const(_) | Node::Property(_) | Node::Negate(_) => false,
    }
}

fn as_boolean(node: Node) -> Result<Node> {
    match &node {
        Node::Property(_) => Ok(Node::BooleanCast(Box::new(node))),
        _ if is_boolean(&node) => Ok(node),
        _ => Err(invalid(format!(
            "expression will not result in a boolean value: {}",
            describe(&node)
        ))),
    }
}

fn describe(node: &Node) -> String {
    match node {
        Node::Const(value) => value.describe(),
        Node::Property(name) => name.clone(),
        Node::Ordered(order, left, right) => format!(
            "({} {} {})",
            describe(left),
            match order {
                Order::Greater => ">",
                Order::GreaterEqual => ">=",
                Order::Less => "<",
                Order::LessEqual => "<=",
            },
            describe(right)
        ),
        Node::Equal(left, right) => format!("({} == {})", describe(left), describe(right)),
        Node::Not(inner) => format!("(NOT {})", describe(inner)),
        Node::And(left, right) => format!("({} && {})", describe(left), describe(right)),
        Node::Or(left, right) => format!("({} || {})", describe(left), describe(right)),
        Node::BooleanCast(inner) => describe(inner),
        Node::Negate(inner) => format!("(- {})", describe(inner)),
        Node::In(prop, list, not) => format!(
            "{} {} ( {} )",
            describe(prop),
            if *not { "NOT IN" } else { "IN" },
            list.iter()
                .map(|value| format!("'{value}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Node::Text(kind, value, search, not) => format!(
            "{} {} '{}'",
            describe(value),
            match kind {
                Affix::Contains if *not => "NOT CONTAINS",
                Affix::Contains => "CONTAINS",
                Affix::StartsWith if *not => "NOT STARTSWITH",
                Affix::StartsWith => "STARTSWITH",
                Affix::EndsWith if *not => "NOT ENDSWITH",
                Affix::EndsWith => "ENDSWITH",
            },
            search.replace('\'', "''")
        ),
    }
}

/// `ComparisonExpression.checkLessThanOperand`, run when an ordering operator is built.
fn check_ordered(node: &Node) -> Result<()> {
    if let Node::Const(value) = node {
        if !value.is_number() {
            return Err(invalid(format!(
                "value '{}' cannot be compared",
                value.describe()
            )));
        }
        return Ok(());
    }
    if is_boolean(node) {
        return Err(invalid(format!(
            "value '{}' cannot be compared",
            describe(node)
        )));
    }
    Ok(())
}

/// `ComparisonExpression.checkEqualOperand` plus the constant compatibility check.
fn check_equal(left: &Node, right: &Node) -> Result<()> {
    for node in [&left, &right] {
        if matches!(node, Node::Const(Value::Null)) {
            return Err(invalid("'NULL' cannot be compared"));
        }
    }
    // Only reached through createEqual; IS NULL builds the same node without these checks.
    if let (Node::Const(left), Node::Const(right)) = (left, right) {
        let left_boolean = matches!(left, Value::Bool(_) | Value::Null);
        let right_boolean = matches!(right, Value::Bool(_) | Value::Null);
        if left_boolean && !right_boolean {
            return Err(invalid(format!(
                "'{}' cannot be compared with '{}'",
                left.describe(),
                right.describe()
            )));
        }
    }
    Ok(())
}

fn create_between(value: Node, low: Node, high: Node) -> Result<Node> {
    if let (Node::Const(low), Node::Const(high)) = (&low, &high) {
        if matches!(low, Value::Null) || matches!(high, Value::Null) {
            return Err(invalid("illegal values of between, values can not be null"));
        }
        // The original compares (high, low) with string conversion enabled, so an
        // unconvertible string bound fails the same way a comparison would.
        let answer = compare(high, low).map_err(invalid)?;
        if answer < 0 {
            return Err(invalid(format!(
                "illegal values of between, left value({low}) must less than or equal to right value({high})",
                low = low.describe(),
                high = high.describe()
            )));
        }
    }
    check_ordered(&value)?;
    check_ordered(&low)?;
    check_ordered(&high)?;
    Ok(Node::And(
        Box::new(Node::Ordered(
            Order::GreaterEqual,
            Box::new(value.clone()),
            Box::new(low),
        )),
        Box::new(Node::Ordered(
            Order::LessEqual,
            Box::new(value),
            Box::new(high),
        )),
    ))
}

fn create_in(value: Node, list: Vec<String>, not: bool) -> Result<Node> {
    let Node::Property(name) = &value else {
        return Err(invalid(format!(
            "expected a property for In expression, got: {}",
            describe(&value)
        )));
    };
    Ok(Node::In(Box::new(Node::Property(name.clone())), list, not))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Keyword {
    Not,
    And,
    Or,
    Between,
    In,
    True,
    False,
    Null,
    Is,
    Contains,
    StartsWith,
    EndsWith,
}

fn keyword(text: &str) -> Option<Keyword> {
    let upper = text.to_ascii_uppercase();
    Some(match upper.as_str() {
        "NOT" => Keyword::Not,
        "AND" => Keyword::And,
        "OR" => Keyword::Or,
        "BETWEEN" => Keyword::Between,
        "IN" => Keyword::In,
        "TRUE" => Keyword::True,
        "FALSE" => Keyword::False,
        "NULL" => Keyword::Null,
        "IS" => Keyword::Is,
        "CONTAINS" => Keyword::Contains,
        "STARTSWITH" => Keyword::StartsWith,
        "ENDSWITH" => Keyword::EndsWith,
        _ => return None,
    })
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Keyword(Keyword),
    Identifier(String),
    Decimal(String),
    Float(String),
    Text(String),
    Punct(&'static str),
}

const PUNCT: [&str; 11] = [">=", "<=", "<>", "=", "<", ">", "+", "-", "(", ")", ","];

fn is_identifier_part(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '$'
}

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut at = 0usize;
    while at < chars.len() {
        let ch = chars[at];
        if matches!(ch, ' ' | '\t' | '\n' | '\r' | '\u{0c}') {
            at += 1;
            continue;
        }
        // The comment pattern requires a terminator, so a trailing `--` falls through to `-`.
        if ch == '-'
            && chars.get(at + 1) == Some(&'-')
            && let Some(end) = (at + 2..chars.len()).find(|&i| matches!(chars[i], '\n' | '\r'))
        {
            at = end + 1;
            continue;
        }
        if ch == '/' && chars.get(at + 1) == Some(&'*') {
            let Some(end) =
                (at + 2..chars.len()).find(|&i| chars[i] == '*' && chars.get(i + 1) == Some(&'/'))
            else {
                return Err(invalid("unterminated block comment"));
            };
            at = end + 2;
            continue;
        }
        if ch == '\'' {
            let mut text = String::new();
            let mut cursor = at + 1;
            loop {
                match chars.get(cursor) {
                    Some('\'') if chars.get(cursor + 1) == Some(&'\'') => {
                        text.push('\'');
                        cursor += 2;
                    }
                    Some('\'') => {
                        cursor += 1;
                        break;
                    }
                    Some(&inner) => {
                        text.push(inner);
                        cursor += 1;
                    }
                    None => return Err(invalid("unterminated string literal")),
                }
            }
            tokens.push(Token::Text(text));
            at = cursor;
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' || ch == '$' {
            let end = identifier_end(&chars, at);
            let run: String = chars[at..end].iter().collect();
            tokens.push(match keyword(&run) {
                // JavaCC breaks an identifier/keyword length tie in favour of the keyword.
                Some(found) => Token::Keyword(found),
                None => Token::Identifier(run),
            });
            at = end;
            continue;
        }
        if ch.is_ascii_digit() || ch == '.' {
            let (token, end) = number(&chars, at)?;
            tokens.push(token);
            at = end;
            continue;
        }
        let Some(punct) = PUNCT
            .iter()
            .find(|punct| chars[at..].starts_with(&punct.chars().collect::<Vec<_>>()))
        else {
            return Err(invalid(format!("unexpected character {ch}")));
        };
        tokens.push(Token::Punct(punct));
        at += punct.chars().count();
    }
    Ok(tokens)
}

fn identifier_end(chars: &[char], from: usize) -> usize {
    let mut end = from;
    while end < chars.len() && is_identifier_part(chars[end]) {
        end += 1;
    }
    end
}

fn digits_end(chars: &[char], from: usize) -> usize {
    let mut end = from;
    while end < chars.len() && chars[end].is_ascii_digit() {
        end += 1;
    }
    end
}

/// JavaCC `EXPONENT`, which needs at least one digit after the optional sign.
fn exponent_end(chars: &[char], from: usize) -> Option<usize> {
    if !chars.get(from).is_some_and(|ch| *ch == 'e' || *ch == 'E') {
        return None;
    }
    let mut cursor = from + 1;
    if chars.get(cursor).is_some_and(|ch| *ch == '+' || *ch == '-') {
        cursor += 1;
    }
    let digits = digits_end(chars, cursor);
    if digits == cursor {
        return None;
    }
    Some(digits)
}

fn number(chars: &[char], at: usize) -> Result<(Token, usize)> {
    if chars[at] == '.' {
        let digits = digits_end(chars, at + 1);
        if digits == at + 1 {
            return Err(invalid("expected digits after '.'"));
        }
        let end = exponent_end(chars, digits).unwrap_or(digits);
        return Ok((Token::Float(chars[at..end].iter().collect()), end));
    }
    let integer = digits_end(chars, at);
    if chars.get(integer) == Some(&'.') {
        let digits = digits_end(chars, integer + 1);
        let end = exponent_end(chars, digits).unwrap_or(digits);
        return Ok((Token::Float(chars[at..end].iter().collect()), end));
    }
    if let Some(end) = exponent_end(chars, integer) {
        return Ok((Token::Float(chars[at..end].iter().collect()), end));
    }
    // The decimal pattern is `"0" | [1-9][0-9]* [lL]?`, so `0L` is not one token.
    if chars[at] == '0' {
        return Ok((Token::Decimal("0".into()), at + 1));
    }
    let end = integer
        + usize::from(
            chars
                .get(integer)
                .is_some_and(|ch| *ch == 'l' || *ch == 'L'),
        );
    Ok((Token::Decimal(chars[at..end].iter().collect()), end))
}

fn parse_decimal(image: &str) -> Result<Value> {
    let digits = image.strip_suffix(['l', 'L']).unwrap_or(image);
    let Ok(wide) = digits.parse::<i64>() else {
        return Err(invalid(format!("{digits} is out of long range")));
    };
    Ok(match i32::try_from(wide) {
        Ok(narrow) => Value::I32(narrow),
        Err(_) => Value::I64(wide),
    })
}

fn parse_float(image: &str) -> Result<Value> {
    let Ok(value) = image.parse::<f64>() else {
        return Err(invalid(format!("invalid double {image}")));
    };
    // Java compares against Double.MAX_VALUE and the smallest positive Double.MIN_VALUE.
    if value > f64::MAX {
        return Err(invalid(format!("{image} is greater than max double")));
    }
    if value < f64::MIN_POSITIVE {
        return Err(invalid(format!("{image} is less than min double")));
    }
    Ok(Value::F64(value))
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn token_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.at + offset)
    }

    fn at_keyword(&self, wanted: Keyword) -> bool {
        self.peek() == Some(&Token::Keyword(wanted))
    }

    fn at_punct(&self, wanted: &str) -> bool {
        matches!(self.peek(), Some(Token::Punct(found)) if *found == wanted)
    }

    fn eat_keyword(&mut self, wanted: Keyword) -> bool {
        if self.at_keyword(wanted) {
            self.at += 1;
            return true;
        }
        false
    }

    fn eat_punct(&mut self, wanted: &str) -> bool {
        if self.at_punct(wanted) {
            self.at += 1;
            return true;
        }
        false
    }

    fn expect_punct(&mut self, wanted: &str) -> Result<()> {
        if self.eat_punct(wanted) {
            Ok(())
        } else {
            Err(invalid(format!("expected {wanted}")))
        }
    }

    fn string_literal(&mut self) -> Result<String> {
        match self.peek() {
            Some(Token::Text(text)) => {
                let text = text.clone();
                self.at += 1;
                Ok(text)
            }
            _ => Err(invalid("expected string literal")),
        }
    }

    fn or_expression(&mut self) -> Result<Node> {
        let mut left = self.and_expression()?;
        while self.eat_keyword(Keyword::Or) {
            let right = self.and_expression()?;
            left = Node::Or(Box::new(as_boolean(left)?), Box::new(as_boolean(right)?));
        }
        Ok(left)
    }

    fn and_expression(&mut self) -> Result<Node> {
        let mut left = self.equality_expression()?;
        while self.eat_keyword(Keyword::And) {
            let right = self.equality_expression()?;
            left = Node::And(Box::new(as_boolean(left)?), Box::new(as_boolean(right)?));
        }
        Ok(left)
    }

    fn equality_expression(&mut self) -> Result<Node> {
        let mut left = self.comparison_expression()?;
        loop {
            if self.eat_punct("=") {
                let right = self.comparison_expression()?;
                check_equal(&left, &right)?;
                left = Node::Equal(Box::new(left), Box::new(right));
            } else if self.eat_punct("<>") {
                let right = self.comparison_expression()?;
                check_equal(&left, &right)?;
                left = Node::Not(Box::new(Node::Equal(Box::new(left), Box::new(right))));
            } else if self.at_keyword(Keyword::Is) {
                // LOOKAHEAD(2) picks IS NULL; anything else still has to be IS NOT NULL,
                // because the outer choice already committed to the IS alternative.
                if self.token_at(1) == Some(&Token::Keyword(Keyword::Null)) {
                    self.at += 2;
                    left = Node::Equal(Box::new(left), Box::new(Node::Const(Value::Null)));
                } else {
                    self.at += 1;
                    if !self.eat_keyword(Keyword::Not) {
                        return Err(invalid("expected NOT or NULL after IS"));
                    }
                    if !self.eat_keyword(Keyword::Null) {
                        return Err(invalid("expected NULL after IS NOT"));
                    }
                    left = Node::Not(Box::new(Node::Equal(
                        Box::new(left),
                        Box::new(Node::Const(Value::Null)),
                    )));
                }
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn comparison_expression(&mut self) -> Result<Node> {
        let mut left = self.unary_expression()?;
        loop {
            if let Some(order) = self.order_operator() {
                self.at += 1;
                let right = self.unary_expression()?;
                check_ordered(&left)?;
                check_ordered(&right)?;
                left = Node::Ordered(order, Box::new(left), Box::new(right));
                continue;
            }
            let affix = if self.at_keyword(Keyword::Contains) {
                self.at += 1;
                (Affix::Contains, false)
            } else if self.at_keyword(Keyword::StartsWith) {
                self.at += 1;
                (Affix::StartsWith, false)
            } else if self.at_keyword(Keyword::EndsWith) {
                self.at += 1;
                (Affix::EndsWith, false)
            } else if self.at_keyword(Keyword::Not) {
                // Every NOT prefixed alternative is guarded by LOOKAHEAD(2), and the
                // generated choice ends in `jj_consume_token(-1)`, so a NOT that is not
                // followed by an affix operator is an error rather than the end of the loop.
                match self.token_at(1) {
                    Some(&Token::Keyword(Keyword::Contains)) => {
                        self.at += 2;
                        (Affix::Contains, true)
                    }
                    Some(&Token::Keyword(Keyword::StartsWith)) => {
                        self.at += 2;
                        (Affix::StartsWith, true)
                    }
                    Some(&Token::Keyword(Keyword::EndsWith)) => {
                        self.at += 2;
                        (Affix::EndsWith, true)
                    }
                    Some(&Token::Keyword(Keyword::Between)) => {
                        self.at += 2;
                        let low = self.unary_expression()?;
                        if !self.eat_keyword(Keyword::And) {
                            return Err(invalid("expected AND in NOT BETWEEN"));
                        }
                        let high = self.unary_expression()?;
                        check_ordered(&left)?;
                        check_ordered(&low)?;
                        let below =
                            Node::Ordered(Order::Less, Box::new(left.clone()), Box::new(low));
                        check_ordered(&high)?;
                        let above = Node::Ordered(Order::Greater, Box::new(left), Box::new(high));
                        left = Node::Or(Box::new(below), Box::new(above));
                        continue;
                    }
                    Some(&Token::Keyword(Keyword::In)) => {
                        self.at += 2;
                        let list = self.in_list()?;
                        left = create_in(left, list, true)?;
                        continue;
                    }
                    _ => {
                        return Err(invalid(
                            "expected CONTAINS, STARTSWITH, ENDSWITH, BETWEEN or IN after NOT",
                        ));
                    }
                }
            } else if self.at_keyword(Keyword::Between) {
                self.at += 1;
                let low = self.unary_expression()?;
                if !self.eat_keyword(Keyword::And) {
                    return Err(invalid("expected AND in BETWEEN"));
                }
                let high = self.unary_expression()?;
                left = create_between(left, low, high)?;
                continue;
            } else if self.at_keyword(Keyword::In) {
                self.at += 1;
                let list = self.in_list()?;
                left = create_in(left, list, false)?;
                continue;
            } else {
                break;
            };
            let search = self.string_literal()?;
            left = Node::Text(affix.0, Box::new(left), search, affix.1);
        }
        Ok(left)
    }

    fn order_operator(&self) -> Option<Order> {
        match self.peek() {
            Some(Token::Punct(">")) => Some(Order::Greater),
            Some(Token::Punct(">=")) => Some(Order::GreaterEqual),
            Some(Token::Punct("<")) => Some(Order::Less),
            Some(Token::Punct("<=")) => Some(Order::LessEqual),
            _ => None,
        }
    }

    fn in_list(&mut self) -> Result<Vec<String>> {
        self.expect_punct("(")?;
        let mut list = vec![self.string_literal()?];
        while self.eat_punct(",") {
            list.push(self.string_literal()?);
        }
        self.expect_punct(")")?;
        Ok(list)
    }

    fn unary_expression(&mut self) -> Result<Node> {
        // Unary plus is accepted but has no node, matching the original action-free branch.
        if self.eat_punct("+") {
            return self.unary_expression();
        }
        if self.eat_punct("-") {
            return Ok(Node::Negate(Box::new(self.unary_expression()?)));
        }
        if self.eat_keyword(Keyword::Not) {
            let inner = self.unary_expression()?;
            return Ok(Node::Not(Box::new(as_boolean(inner)?)));
        }
        self.primary_expression()
    }

    fn primary_expression(&mut self) -> Result<Node> {
        match self.peek() {
            Some(Token::Text(_) | Token::Decimal(_) | Token::Float(_))
            | Some(Token::Keyword(Keyword::True | Keyword::False | Keyword::Null)) => {
                self.literal()
            }
            Some(Token::Identifier(name)) => {
                let name = name.clone();
                self.at += 1;
                Ok(Node::Property(name))
            }
            Some(Token::Punct("(")) => {
                self.at += 1;
                let inner = self.or_expression()?;
                self.expect_punct(")")?;
                Ok(inner)
            }
            _ => Err(invalid(
                "expected a literal, property or bracketed expression",
            )),
        }
    }

    fn literal(&mut self) -> Result<Node> {
        let node = match self.peek() {
            Some(Token::Text(text)) => Node::Const(Value::Str(text.clone())),
            Some(Token::Decimal(image)) => Node::Const(parse_decimal(image)?),
            Some(Token::Float(image)) => Node::Const(parse_float(image)?),
            Some(Token::Keyword(Keyword::True)) => Node::Const(Value::Bool(true)),
            Some(Token::Keyword(Keyword::False)) => Node::Const(Value::Bool(false)),
            Some(Token::Keyword(Keyword::Null)) => Node::Const(Value::Null),
            _ => return Err(invalid("expected a literal")),
        };
        self.at += 1;
        Ok(node)
    }
}

/// Compile a SQL92 filter. Trailing tokens after a complete expression are ignored,
/// because the original start production never matches EOF.
pub fn compile(sql: &str) -> Result<Expression> {
    let tokens = tokenize(sql)?;
    let mut parser = Parser { tokens, at: 0 };
    let root = as_boolean(parser.or_expression()?)?;
    Ok(Expression { root })
}

#[derive(Clone, Debug, PartialEq)]
pub struct Expression {
    root: Node,
}

type Evaluated = std::result::Result<Value, String>;

impl Expression {
    /// The original broker treats any evaluation failure as a non-match.
    pub fn matches(&self, properties: &BTreeMap<String, String>) -> bool {
        matches!(self.evaluate(properties), Ok(Value::Bool(true)))
    }

    pub fn evaluate(&self, properties: &BTreeMap<String, String>) -> Evaluated {
        evaluate(&self.root, properties)
    }

    pub fn properties(&self) -> Vec<String> {
        let mut names = Vec::new();
        collect_properties(&self.root, &mut names);
        names.sort();
        names.dedup();
        names
    }

    pub fn text(&self) -> String {
        describe(&self.root)
    }
}

fn collect_properties(node: &Node, names: &mut Vec<String>) {
    match node {
        Node::Property(name) => names.push(name.clone()),
        Node::Const(_) => {}
        Node::BooleanCast(inner) | Node::Negate(inner) | Node::Not(inner) => {
            collect_properties(inner, names);
        }
        Node::In(prop, _, _) | Node::Text(_, prop, _, _) => collect_properties(prop, names),
        Node::Ordered(_, left, right)
        | Node::Equal(left, right)
        | Node::And(left, right)
        | Node::Or(left, right) => {
            collect_properties(left, names);
            collect_properties(right, names);
        }
    }
}

fn evaluate(node: &Node, properties: &BTreeMap<String, String>) -> Evaluated {
    match node {
        Node::Const(value) => Ok(value.clone()),
        Node::Property(name) => Ok(properties
            .get(name)
            .map_or(Value::Null, |value| Value::Str(value.clone()))),
        Node::Ordered(order, left, right) => {
            let left = evaluate(left, properties)?;
            if left == Value::Null {
                return Ok(Value::Null);
            }
            let right = evaluate(right, properties)?;
            if right == Value::Null {
                return Ok(Value::Null);
            }
            // Ordering two strings is illegal unless both convert to double.
            let answer = match (&left, &right) {
                (Value::Str(a), Value::Str(b)) => {
                    let a = to_double(a)?;
                    let b = to_double(b)?;
                    sign(compare_doubles(a, b))
                }
                _ => compare(&left, &right)?,
            };
            Ok(Value::Bool(match order {
                Order::Greater => answer > 0,
                Order::GreaterEqual => answer >= 0,
                Order::Less => answer < 0,
                Order::LessEqual => answer <= 0,
            }))
        }
        Node::Equal(left, right) => {
            let left = evaluate(left, properties)?;
            let right = evaluate(right, properties)?;
            // Exactly one null is unknown; two nulls are equal.
            if (left == Value::Null) != (right == Value::Null) {
                return Ok(if left == Value::Null {
                    Value::Null
                } else {
                    Value::Bool(false)
                });
            }
            if left == right {
                return Ok(Value::Bool(true));
            }
            Ok(Value::Bool(compare(&left, &right)? == 0))
        }
        Node::Not(inner) => match evaluate(inner, properties)? {
            Value::Null => Ok(Value::Null),
            Value::Bool(value) => Ok(Value::Bool(!value)),
            _ => Err("not a boolean value".into()),
        },
        Node::And(left, right) => {
            let left = evaluate(left, properties)?;
            if left == Value::Bool(false) {
                return Ok(Value::Bool(false));
            }
            let right = evaluate(right, properties)?;
            if right == Value::Bool(false) {
                return Ok(Value::Bool(false));
            }
            if left == Value::Null || right == Value::Null {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(true))
        }
        Node::Or(left, right) => {
            let left = evaluate(left, properties)?;
            if left == Value::Bool(true) {
                return Ok(Value::Bool(true));
            }
            let right = evaluate(right, properties)?;
            if right == Value::Bool(true) {
                return Ok(Value::Bool(true));
            }
            if left == Value::Null || right == Value::Null {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(false))
        }
        Node::BooleanCast(inner) => match evaluate(inner, properties)? {
            Value::Null => Ok(Value::Null),
            Value::Bool(value) => Ok(Value::Bool(value)),
            _ => Ok(Value::Bool(false)),
        },
        Node::Negate(inner) => match evaluate(inner, properties)? {
            Value::I32(value) => Ok(Value::I32(value.wrapping_neg())),
            Value::I64(value) => Ok(Value::I64(value.wrapping_neg())),
            Value::F64(value) => Ok(Value::F64(-value)),
            Value::Bool(value) => Err(format!("don't know how to negate: {value}")),
            Value::Null | Value::Str(_) => Ok(Value::Null),
        },
        Node::In(prop, list, not) => {
            // The original returns null for a missing or non-string value.
            match evaluate(prop, properties)? {
                Value::Str(text) => Ok(Value::Bool(list.iter().any(|item| item == &text) != *not)),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        Node::Text(kind, value, search, not) => {
            if search.is_empty() {
                return Ok(Value::Bool(false));
            }
            let text = match evaluate(value, properties)? {
                Value::Str(text) => text,
                _ => return Ok(Value::Bool(false)),
            };
            let answer = match kind {
                Affix::Contains => text.contains(search.as_str()),
                Affix::StartsWith => text.starts_with(search.as_str()),
                Affix::EndsWith => text.ends_with(search.as_str()),
            };
            Ok(Value::Bool(answer != *not))
        }
    }
}

/// `ComparisonExpression.__compare` with string conversion always enabled.
/// Pairs Java cannot align yield -1 instead of an error, so `<` over them is true.
pub(crate) fn compare(left: &Value, right: &Value) -> std::result::Result<i32, String> {
    let aligned: (Value, Value) = match (left, right) {
        (Value::Bool(_), Value::Str(text)) => (left.clone(), Value::Bool(to_bool(text))),
        (Value::I32(_), Value::Str(text)) => (left.clone(), Value::I32(to_integer(text)?)),
        (Value::I64(_), Value::Str(text)) => (left.clone(), Value::I64(to_long(text)?)),
        (Value::F64(_), Value::Str(text)) => (left.clone(), Value::F64(to_double(text)?)),
        (Value::Str(text), Value::Bool(_)) => (Value::Bool(to_bool(text)), right.clone()),
        (Value::Str(text), Value::I32(_)) => (Value::I32(to_integer(text)?), right.clone()),
        (Value::Str(text), Value::I64(_)) => (Value::I64(to_long(text)?), right.clone()),
        (Value::Str(text), Value::F64(_)) => (Value::F64(to_double(text)?), right.clone()),
        // Numbers widen towards the wider type, exactly like the original branches.
        (Value::I32(value), Value::I64(_)) => (Value::I64(i64::from(*value)), right.clone()),
        (Value::I32(value), Value::F64(_)) => (Value::F64(f64::from(*value)), right.clone()),
        (Value::I64(value), Value::F64(_)) => (Value::F64(*value as f64), right.clone()),
        (Value::I64(_), Value::I32(value)) => (left.clone(), Value::I64(i64::from(*value))),
        (Value::F64(_), Value::I32(value)) => (left.clone(), Value::F64(f64::from(*value))),
        (Value::F64(_), Value::I64(value)) => (left.clone(), Value::F64(*value as f64)),
        _ if std::mem::discriminant(left) == std::mem::discriminant(right) => {
            (left.clone(), right.clone())
        }
        _ => return Ok(-1),
    };
    Ok(ordered(&aligned.0, &aligned.1))
}

fn ordered(left: &Value, right: &Value) -> i32 {
    let answer = match (left, right) {
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Str(a), Value::Str(b)) => utf16_cmp(a, b),
        (Value::I32(a), Value::I32(b)) => a.cmp(b),
        (Value::I64(a), Value::I64(b)) => a.cmp(b),
        (Value::F64(a), Value::F64(b)) => compare_doubles(*a, *b),
        _ => return -1,
    };
    sign(answer)
}

fn sign(answer: Ordering) -> i32 {
    match answer {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// Java `Double.compareTo`: NaN equals NaN and orders above every number.
fn compare_doubles(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => left.total_cmp(&right),
    }
}

fn to_bool(text: &str) -> bool {
    text.eq_ignore_ascii_case("true")
}

fn to_integer(text: &str) -> std::result::Result<i32, String> {
    text.parse::<i32>()
        .map_err(|_| format!("invalid int {text}"))
}

fn to_long(text: &str) -> std::result::Result<i64, String> {
    text.parse::<i64>()
        .map_err(|_| format!("invalid long {text}"))
}

fn to_double(text: &str) -> std::result::Result<f64, String> {
    // Java trims and accepts one trailing d/f suffix; hex float literals are not accepted.
    let trimmed = text
        .trim_start_matches([' ', '\t', '\n', '\r', '\u{0c}'])
        .trim_end_matches([' ', '\t', '\n', '\r', '\u{0c}']);
    let cleaned = trimmed
        .strip_suffix(['d', 'D', 'f', 'F'])
        .unwrap_or(trimmed);
    cleaned
        .parse::<f64>()
        .map_err(|_| format!("invalid double {text}"))
}

/// Java `String.compareTo` orders UTF-16 code units, not Unicode scalars.
fn utf16_cmp(left: &str, right: &str) -> Ordering {
    let units = |text: &str| {
        let mut out = Vec::with_capacity(text.len());
        for ch in text.chars() {
            let mut buffer = [0u16; 2];
            out.extend_from_slice(ch.encode_utf16(&mut buffer));
        }
        out
    };
    units(left).cmp(&units(right))
}

/// The broker's `FilterAPI.build` plus `ConsumerFilterManager` result for a subscription.
#[derive(Clone, Debug)]
pub enum Subscription {
    All,
    Tags(Vec<String>),
    Sql(Expression),
}

/// `ExpressionType.isTagType`: only the exact "TAG" spelling counts.
pub fn is_tag_type(expression_type: Option<&str>) -> bool {
    matches!(expression_type, None | Some("") | Some("TAG"))
}

impl Subscription {
    pub fn build(expression_type: Option<&str>, substring: &str) -> Result<Self> {
        if is_tag_type(expression_type) {
            if substring.is_empty() || substring == "*" {
                return Ok(Self::All);
            }
            return Ok(Self::Tags(
                substring
                    .split("||")
                    .map(str::trim)
                    .filter(|tag| !tag.is_empty())
                    .map(str::to_owned)
                    .collect(),
            ));
        }
        if substring.is_empty() {
            return Err(invalid(format!(
                "expression can't be null for type {expression_type:?}"
            )));
        }
        if expression_type != Some("SQL92") {
            return Err(invalid(format!(
                "unsupported expression type {expression_type:?}"
            )));
        }
        Ok(Self::Sql(compile(substring)?))
    }

    pub fn is_tag(&self) -> bool {
        matches!(self, Self::All | Self::Tags(_))
    }

    /// The original matches tags through a 32 bit `codeSet` in the consume queue and falls back to
    /// the exact `TAGS` property here; comparing the property alone is equivalent for single tag
    /// messages, which is all this store writes.
    pub fn matches(&self, message: &crate::common::Message) -> bool {
        match self {
            Self::All => true,
            Self::Tags(tags) => message
                .properties
                .get("TAGS")
                .is_some_and(|tag| tags.iter().any(|candidate| candidate == tag)),
            Self::Sql(expression) => expression.matches(&message.properties),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Message;

    fn properties(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn value(sql: &str, props: &BTreeMap<String, String>) -> Value {
        compile(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .evaluate(props)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
    }

    fn matched(sql: &str, props: &BTreeMap<String, String>) -> bool {
        compile(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .matches(props)
    }

    fn fails(sql: &str) -> String {
        match compile(sql) {
            Err(error) => error.to_string(),
            Ok(expression) => panic!("expected {sql} to be rejected: {}", expression.text()),
        }
    }

    fn error(sql: &str, props: &BTreeMap<String, String>) -> String {
        compile(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .evaluate(props)
            .expect_err("expected evaluation to fail")
    }

    #[test]
    fn tokenizer_accepts_comments_escapes_and_keyword_boundaries() {
        let props = properties(&[("a", "1")]);
        assert!(matched("a=1 -- trailing comment\n", &props));
        assert!(matched("/* skip */ a = /* inner */ 1", &props));
        // A line comment needs a terminator, so this is two minus tokens, which the
        // start rule then ignores as trailing input.
        assert!(matched("a=1 --b", &props));
        assert!(matched("'it''s' = 'it''s'", &properties(&[])));
        assert_eq!(
            compile("'' = 'a'")
                .unwrap()
                .evaluate(&properties(&[]))
                .unwrap(),
            Value::Bool(false)
        );
        // A longer identifier wins over a keyword prefix.
        assert!(matched("NOTHING = 1", &properties(&[("NOTHING", "1")])));
        assert!(matched("_a = 1", &properties(&[("_a", "1")])));
        assert!(matched("$a = 1", &properties(&[("$a", "1")])));
        assert!(fails("and = 1").contains("expected"));
        assert!(fails("非 = 1").contains("unexpected character"));
    }

    #[test]
    fn literal_tokens_follow_the_grammar() {
        assert!(matched("a = 0", &properties(&[("a", "0")])));
        // The decimal rule is `"0" | [1-9][0-9]*`, so a leading zero ends the token.
        assert!(matched("a = 007", &properties(&[("a", "0")])));
        assert!(!matched("a = 007", &properties(&[("a", "7")])));
        assert!(matched("a = 107", &properties(&[("a", "107")])));
        // "0" cannot take a long suffix, so the L starts a separate token.
        assert_eq!(
            tokenize("0L").unwrap(),
            vec![Token::Decimal("0".into()), Token::Identifier("L".into())]
        );
        assert_eq!(tokenize("12L").unwrap(), vec![Token::Decimal("12L".into())]);
        assert_eq!(
            tokenize("1.5E-3").unwrap(),
            vec![Token::Float("1.5E-3".into())]
        );
        assert_eq!(tokenize("1E5").unwrap(), vec![Token::Float("1E5".into())]);
        assert_eq!(
            tokenize("1e").unwrap(),
            vec![Token::Decimal("1".into()), Token::Identifier("e".into())]
        );
        // The floating rule also matches ".2", so the second dot starts a new number.
        assert_eq!(
            tokenize("5.5.2").unwrap(),
            vec![Token::Float("5.5".into()), Token::Float(".2".into())]
        );
        assert!(matched("a = 1E5", &properties(&[("a", "100000")])));
        assert!(fails("a = 9223372036854775808").contains("out of long range"));
        assert!(matched(
            "a = 9223372036854775807",
            &properties(&[("a", "9223372036854775807")])
        ));
        // Java rejects a double outside the positive Double range, including zero.
        assert!(fails("a = 0.0").contains("less than min double"));
        assert!(fails("a = 1e-400").contains("less than min double"));
        assert!(fails("a = 1e400").contains("greater than max double"));
        assert!(matched("a = 5.5", &properties(&[("a", "5.5")])));
    }

    #[test]
    fn three_valued_logic_and_null_propagation() {
        let missing = properties(&[]);
        let one = properties(&[("a", "1")]);
        assert_eq!(value("a = '1'", &missing), Value::Null);
        assert_eq!(value("a <> '1'", &missing), Value::Null);
        assert_eq!(value("a = '1'", &one), Value::Bool(true));
        assert_eq!(value("a <> '1'", &one), Value::Bool(false));
        assert!(matched("a IS NULL", &missing));
        assert!(!matched("a IS NULL", &one));
        assert!(matched("a IS NOT NULL", &one));
        // Two nulls are equal, so NOT of a missing IS NULL is false rather than unknown.
        assert_eq!(value("a IS NOT NULL", &missing), Value::Bool(false));
        // false AND unknown is false; unknown AND unknown stays unknown.
        assert_eq!(
            value("b = '2' AND a = '1'", &properties(&[("b", "5")])),
            Value::Bool(false)
        );
        assert_eq!(value("a = '2' AND a = '1'", &missing), Value::Null);
        assert_eq!(value("a = '1' OR a = '2'", &missing), Value::Null);
        assert_eq!(value("NOT a = '1'", &missing), Value::Null);
        assert_eq!(value("a > 1", &missing), Value::Null);
        assert_eq!(value("a > 1", &one), Value::Bool(false));
    }

    #[test]
    fn bare_constants_cast_to_boolean() {
        let all = properties(&[("flag", "true"), ("text", "x")]);
        // TRUE, FALSE and NULL literals are BooleanConstantExpression in the original.
        assert!(matched("TRUE", &properties(&[])));
        assert!(!matched("FALSE", &properties(&[])));
        assert!(!matched("NULL", &properties(&[])));
        // A property value is always a String, so the boolean cast is never true.
        assert!(!matched("flag", &all));
        assert!(!matched("missing", &all));
        assert_eq!(value("NOT NULL", &properties(&[])), Value::Null);
        assert!(fails("1").contains("will not result in a boolean value"));
        assert!(fails("'text'").contains("will not result in a boolean value"));
        assert!(fails("flag AND 1").contains("will not result in a boolean value"));
        assert!(fails("-5").contains("will not result in a boolean value"));
    }

    #[test]
    fn string_coercion_and_illegal_comparisons() {
        assert!(matched("a > b", &properties(&[("a", "10"), ("b", "9")])));
        assert!(!matched("a < b", &properties(&[("a", "10"), ("b", "9")])));
        // Two strings compared by an ordering operator must convert to double.
        assert!(error("a > b", &properties(&[("a", "x"), ("b", "9")])).contains("invalid double"));
        assert!(!matched("a > b", &properties(&[("a", "x"), ("b", "9")])));
        assert_eq!(
            value("a = '10'", &properties(&[("a", "10")])),
            Value::Bool(true)
        );
        assert_eq!(
            value("a = 'b'", &properties(&[("a", "B")])),
            Value::Bool(false)
        );
        assert!(fails("a > 'text'").contains("cannot be compared"));
        assert!(fails("a > TRUE").contains("cannot be compared"));
        assert!(fails("'text' > 1").contains("cannot be compared"));
        assert!(fails("a > (b = 1)").contains("cannot be compared"));
        assert!(fails("a = NULL").contains("'NULL' cannot be compared"));
        assert!(fails("NULL = a").contains("'NULL' cannot be compared"));
    }

    #[test]
    fn unalignable_type_pairs_compare_as_minus_one() {
        // The compatibility check only rejects a boolean constant on the left.
        assert!(fails("TRUE = 1").contains("cannot be compared with"));
        assert_eq!(value("1 = TRUE", &properties(&[])), Value::Bool(false));
        // A boolean string against a boolean constant aligns through Boolean.valueOf.
        assert_eq!(
            value("flag = TRUE", &properties(&[("flag", "true")])),
            Value::Bool(true)
        );
        assert_eq!(
            value("flag = TRUE", &properties(&[("flag", "anything")])),
            Value::Bool(false)
        );
        // A number-shaped string is parsed as an int, which fails for a fraction.
        assert!(error("a = 1", &properties(&[("a", "1.0")])).contains("invalid int"));
        assert!(error("a > 1", &properties(&[("a", "abc")])).contains("invalid int"));
    }

    #[test]
    fn between_and_in_use_the_original_expansions() {
        let two = properties(&[("a", "2")]);
        assert!(matched("a BETWEEN 1 AND 3", &two));
        assert!(!matched("a NOT BETWEEN 1 AND 3", &two));
        // A negated between is an OR of the outer comparisons, bounds included.
        assert!(!matched(
            "a NOT BETWEEN 1 AND 3",
            &properties(&[("a", "3")])
        ));
        assert!(matched("a NOT BETWEEN 1 AND 3", &properties(&[("a", "4")])));
        assert!(fails("a BETWEEN 3 AND 1").contains("must less than or equal to right value"));
        assert!(fails("a BETWEEN NULL AND 1").contains("can not be null"));
        assert!(fails("a BETWEEN 'x' AND 1").contains("invalid int"));
        // A numeric constant passes the operand check, so the range test just fails.
        assert!(!matched("1 BETWEEN 2 AND 3", &properties(&[])));
        assert!(!matched("a BETWEEN 1 AND 3", &properties(&[])));
        assert!(matched("a IN ('1', '2')", &two));
        assert!(matched("a NOT IN ('1', '3')", &two));
        assert!(!matched("a IN ('1')", &properties(&[("a", "01")])));
        assert_eq!(value("a IN ('1')", &two), Value::Bool(false));
        assert_eq!(value("a IN ('2')", &two), Value::Bool(true));
        assert_eq!(value("a IN ('1')", &properties(&[])), Value::Null);
        assert_eq!(
            value("a IN ('1')", &properties(&[("a", "1"), ("b", "2")])),
            Value::Bool(true)
        );
        assert!(fails("'text' IN ('a')").contains("expected a property"));
        assert!(fails("a IN ()").contains("expected string literal"));
    }

    #[test]
    fn text_operators_never_propagate_null() {
        let props = properties(&[("a", "hello world"), ("n", "5")]);
        assert!(matched("a CONTAINS 'lo'", &props));
        assert!(!matched("a NOT CONTAINS 'lo'", &props));
        assert!(matched("a NOT CONTAINS 'zz'", &props));
        assert!(matched("a STARTSWITH 'hello'", &props));
        assert!(matched("a ENDSWITH 'world'", &props));
        assert!(matched("a NOT STARTSWITH 'xyz'", &props));
        // A property is always a string, so a numeric-looking value still matches.
        assert_eq!(value("n CONTAINS '5'", &props), Value::Bool(true));
        for sql in [
            "a CONTAINS ''",
            "missing CONTAINS 'x'",
            "n NOT CONTAINS '5'",
        ] {
            assert_eq!(value(sql, &props), Value::Bool(false), "{sql}");
        }
        // The NOT forms report false for a missing property instead of inverting it.
        for sql in ["a NOT CONTAINS ''", "missing NOT CONTAINS 'x'"] {
            assert_eq!(value(sql, &props), Value::Bool(false), "{sql}");
        }
        assert!(!matched("missing NOT STARTSWITH 'x'", &props));
        assert!(fails("a CONTAINS 5").contains("expected string literal"));
    }

    #[test]
    fn unary_operators_and_precedence() {
        assert!(matched("a > -1", &properties(&[("a", "0")])));
        assert!(matched("a = - 5", &properties(&[("a", "-5")])));
        assert!(matched("a = + 5", &properties(&[("a", "5")])));
        // A negation of a non-number is null, which IS NULL then reports.
        assert!(matched("(- missing) IS NULL", &properties(&[])));
        assert!(matched("(- 'text') IS NULL", &properties(&[])));
        assert!(matched("(- a) IS NULL", &properties(&[("a", "3")])));
        assert!(!matched("(- 3) IS NULL", &properties(&[])));
        // Negating a boolean is a runtime failure in the original engine.
        assert!(error("a = - TRUE", &properties(&[("a", "1")])).contains("negate"));
        assert!(matched("TRUE OR FALSE AND FALSE", &properties(&[])));
        assert!(!matched("(TRUE OR FALSE) AND FALSE", &properties(&[])));
        assert!(matched("a=1 AND b=2 OR c=3", &properties(&[("c", "3")])));
        assert!(fails("a > b > c").contains("cannot be compared"));
        assert!(fails("a = ").contains("expected a literal"));
        assert!(fails("a > (b").contains("expected )"));
    }

    #[test]
    fn trailing_input_is_ignored_by_the_start_rule() {
        // JmsSelector never matches EOF, so a tail that no loop accepts ends the parse.
        assert!(matched("a = 1 trailing junk", &properties(&[("a", "1")])));
        // The IS alternative and the NOT prefixed affixes commit on their first token,
        // which the differential run against the original parser confirmed as an error.
        assert!(fails("a NOT b").contains("after NOT"));
        assert!(fails("a IS foo").contains("after IS"));
        assert!(fails("a IS").contains("after IS"));
        assert!(fails("a IS NOT").contains("after IS NOT"));
        assert!(compile("a AND").is_err());
    }

    #[test]
    fn string_ordering_uses_utf16_code_units() {
        assert_eq!(
            compare(
                &Value::Str("\u{10000}".into()),
                &Value::Str("\u{ffff}".into())
            ),
            Ok(-1)
        );
        assert_eq!(
            compare(&Value::Str("A".into()), &Value::Str("a".into())),
            Ok(-1)
        );
        assert_eq!(
            compare(&Value::Str("a".into()), &Value::Str("a".into())),
            Ok(0)
        );
    }

    #[test]
    fn expression_reports_referenced_properties() {
        let expression = compile("a > 1 AND (b IN ('x') OR c CONTAINS 'y') AND -d < 2")
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(expression.properties(), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn tag_subscriptions_follow_filter_api_build() {
        assert!(matches!(
            Subscription::build(None, "").unwrap(),
            Subscription::All
        ));
        assert!(matches!(
            Subscription::build(Some("TAG"), "*").unwrap(),
            Subscription::All
        ));
        // Only the exact untrimmed star means all; a padded star becomes a tag.
        assert!(matches!(
            Subscription::build(Some("TAG"), " * ").unwrap(),
            Subscription::Tags(tags) if tags == vec!["*".to_string()]
        ));
        let tags = match Subscription::build(Some("TAG"), " A || B ||  ").unwrap() {
            Subscription::Tags(tags) => tags,
            other => panic!("{other:?}"),
        };
        assert_eq!(tags, vec!["A".to_string(), "B".to_string()]);
        assert!(is_tag_type(Some("TAG")));
        assert!(!is_tag_type(Some("tag")));
        assert!(!is_tag_type(Some("SQL92")));
    }

    #[test]
    fn sql_subscription_compiles_or_rejects() {
        let expression =
            match Subscription::build(Some("SQL92"), "a > 1 AND b IS NOT NULL").unwrap() {
                Subscription::Sql(expression) => expression,
                other => panic!("{other:?}"),
            };
        assert_eq!(expression.properties(), vec!["a", "b"]);
        assert!(Subscription::build(Some("SQL92"), "a >").is_err());
        assert!(Subscription::build(Some("SQL92"), "").is_err());
        // An unknown type has no filter implementation, so no filter data is kept.
        assert!(Subscription::build(Some("OTHER"), "a=1").is_err());
    }

    #[test]
    fn subscription_matches_message_properties() {
        let message = Message {
            properties: properties(&[("TAGS", "blue"), ("level", "7")]),
            ..Message::default()
        };
        assert!(
            Subscription::build(Some("TAG"), "red || blue")
                .unwrap()
                .matches(&message)
        );
        assert!(
            !Subscription::build(Some("TAG"), "red")
                .unwrap()
                .matches(&message)
        );
        assert!(
            !Subscription::build(Some("TAG"), "||")
                .unwrap()
                .matches(&message)
        );
        assert!(
            Subscription::build(Some("SQL92"), "level > 5")
                .unwrap()
                .matches(&message)
        );
        // A broken evaluation is a non-match, not an error, like the original filter.
        assert!(
            !Subscription::build(Some("SQL92"), "TAGS > 5")
                .unwrap()
                .matches(&message)
        );
        assert!(Subscription::All.matches(&message));
    }

    #[test]
    fn evaluate_errors_are_reported_to_callers() {
        let expression = compile("a > 5").unwrap();
        assert_eq!(
            expression.evaluate(&properties(&[("a", "text")])),
            Err("invalid int text".into())
        );
        assert!(!expression.matches(&properties(&[("a", "text")])));
        assert_eq!(expression.evaluate(&properties(&[])), Ok(Value::Null));
        assert_eq!(
            expression.evaluate(&properties(&[("a", "6")])),
            Ok(Value::Bool(true))
        );
    }
}
