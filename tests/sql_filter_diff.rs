//! Differential test of the Rust SQL92 engine against the original Java parser.
//!
//! The oracle is `tests/sqlfilter/SqlFilterDiff.java`, which drives the untouched
//! `org.apache.rocketmq.filter.parser.SelectorParser` from the reference tree. The test compiles and
//! runs it when the Java reference (and a JDK) is available, and skips otherwise so `cargo test`
//! stays runnable without a JDK.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::{env, fs, thread};

use rocketmq_rs::filter::{self, Value};

/// Override with `ROCKETMQ_JAVA_HOME` when the reference tree lives elsewhere.
const DEFAULT_REFERENCE: &str = "/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq";

const PROPERTIES: [&[(&str, &str)]; 7] = [
    &[],
    &[
        ("blank", ""),
        ("dot", ".5"),
        ("exp", "1E3"),
        ("flag", "true"),
        ("huge", "9007199254740993"),
        ("level", "10"),
        ("name", "Alice"),
        ("neg", "-5"),
        ("price", "3.5"),
        ("tags", "hello world"),
        ("up", "TRUE"),
        ("weird", "abc"),
        ("word", "cat"),
        ("zero", "0"),
        ("channel", "web"),
    ],
    &[
        ("blank", "a"),
        ("dot", "1.0"),
        ("exp", "2e-3"),
        ("flag", "FALSE"),
        ("huge", "1"),
        ("level", "5"),
        ("name", "Bob"),
        ("neg", "0"),
        ("price", "2"),
        ("tags", "Goodbye moon"),
        ("up", "false"),
        ("weird", ""),
        ("word", "dog"),
        ("zero", "1"),
        ("channel", "app"),
    ],
    &[("level", "0")],
    &[("name", ""), ("tags", ""), ("level", "-1")],
    &[("name", "'"), ("level", "3.5"), ("price", "abc")],
    &[("x", "1"), ("y", "2"), ("z", "3")],
];

const EXPRESSIONS: &[&str] = &[
    // Comparisons against numeric, string and missing properties.
    "level > 5",
    "level >= 5",
    "level < 5",
    "level <= 5",
    "level = 5",
    "level <> 5",
    "level > '5'",
    "level <= '10.0'",
    "missing > 5",
    "missing = 5",
    "missing <> 5",
    "name > 'Alice'",
    "name < 'Bob'",
    "name = 'Alice'",
    "name = 5",
    "weird > 5",
    "weird < 5",
    "dot > 0.4",
    "exp > 999",
    "huge > 9007199254740992",
    "zero = 0",
    "neg >= -5",
    "price >= '2'",
    "blank = ''",
    "blank <> ''",
    "word <> 'cat'",
    "name = 'It''s'",
    // Booleans, NULL and three valued logic.
    "flag = TRUE",
    "flag = FALSE",
    "flag <> TRUE",
    "flag = 'true'",
    "up = TRUE",
    "TRUE",
    "FALSE",
    "NULL",
    "TRUE AND FALSE",
    "TRUE OR FALSE",
    "NOT TRUE",
    "NULL IS NULL",
    "NULL = NULL",
    "NULL <> NULL",
    "NULL > 5",
    "level > NULL",
    "level IS NULL",
    "level IS NOT NULL",
    "missing IS NULL",
    "blank IS NULL",
    "name IS TRUE",
    // Text predicates.
    "tags CONTAINS 'lo'",
    "tags NOT CONTAINS 'lo'",
    "tags STARTSWITH 'hel'",
    "tags NOT STARTSWITH 'hel'",
    "tags ENDSWITH 'world'",
    "tags NOT ENDSWITH 'moon'",
    "tags CONTAINS ''",
    "missing CONTAINS 'x'",
    "name CONTAINS ''''",
    "level CONTAINS '1'",
    // Set and range predicates.
    "name IN ('Alice', 'Bob')",
    "name NOT IN ('Alice')",
    "level IN ('10', '5')",
    "missing IN ('a')",
    "missing NOT IN ('a')",
    "1 IN ('1')",
    "level BETWEEN 1 AND 20",
    "level NOT BETWEEN 1 AND 20",
    "level BETWEEN 20 AND 1",
    "level BETWEEN 'a' AND 'z'",
    "name BETWEEN 'A' AND 'Z'",
    "price BETWEEN 1 AND 10",
    "x BETWEEN 1 AND 2",
    // Arithmetic and grouping.
    "level + 1 > 10",
    "level - 1 > 8",
    "-level > -10",
    "+level > 5",
    "level * 2 > 5",
    "0 > -0",
    ".5 > 0",
    "5. > 4",
    "5E2 > 400",
    "missing + 1 > 5",
    "(level > 5 OR name = 'Bob') AND level < 100",
    "((level)) > 5",
    "level > 5 AND (name = 'Alice' OR missing = 1)",
    "NOT (name IN ('Alice'))",
    "NOT level",
    "level",
    "TRUE = level > 5",
    "level IS NOT NULL AND level > 0",
    // Malformed input, which the broker turns into a non-matching or rejected subscription.
    "level > 5 AND",
    "level > 'x'",
    "level > 5 AND level > 3",
    "level > 5 -- unterminated comment",
    "level /* c */ > 5",
    "level > 5 /* unterminated",
    "a = 1 trailing junk",
    "a = 1; b = 2",
    "'abc'",
    "1",
    "level @ 5",
    "level =~ 5",
    "IN level",
    "name IN ()",
    "name IN (1)",
    "level BETWEEN NULL AND 3",
    // The IS alternative commits once the parser sees IS.
    "name IS",
    "name IS NOT",
    "name IS NOT TRUE",
    "name IS NOTNULL",
    "name IS NULL IS NULL",
    "level IS NOT NULL > 5",
    // Precedence, associativity and repeated operators.
    "TRUE OR FALSE AND FALSE",
    "NOT TRUE AND TRUE",
    "NOT NOT level",
    "1 = 1 = 1",
    "level > 5 > 3",
    "5 > level",
    "a = -1",
    "'' = name",
    "'a' + 'b' > 0",
    "TRUE AND name",
    "x = 1 AND y = 2 OR z = 3",
    "name > 'Z' AND name < 'a'",
    "level > 5 OR level > 5 AND name = 'Alice'",
    // Numeric literal edges.
    "level > 2147483648",
    "level > 99999999999999999999999",
    "07 > 5",
    "1l > 0",
    "1L > 0",
    ".5.5 > 0",
    "0x1 > 0",
    "1e > 0",
    "1e+ > 0",
    // Operator arguments must be string literals or expressions.
    "tags CONTAINS level",
    "name IN ('a' 'b')",
    "name IN ('a',)",
    "level BETWEEN 1 OR 3",
    "level NOT 5",
];

fn reference_tree() -> Option<PathBuf> {
    let tree =
        PathBuf::from(env::var("ROCKETMQ_JAVA_HOME").unwrap_or_else(|_| DEFAULT_REFERENCE.into()));
    if tree
        .join("filter/src/main/java/org/apache/rocketmq/filter/parser/SelectorParser.java")
        .is_file()
    {
        return Some(tree);
    }
    None
}

/// The packaged jars of the reference build, which carry the original filter parser.
fn distribution_lib(tree: &Path) -> Option<PathBuf> {
    let target = tree.join("distribution/target");
    let entries = fs::read_dir(&target).ok()?;
    for entry in entries.flatten() {
        if !entry.file_name().to_string_lossy().starts_with("rocketmq-") {
            continue;
        }
        for candidate in [
            entry.path().join("lib"),
            entry.path().join("rocketmq-5.5.1/lib"),
        ] {
            if candidate.is_dir() {
                return Some(candidate);
            }
        }
    }
    None
}

fn has_jar(lib: &Path) -> bool {
    fs::read_dir(lib)
        .into_iter()
        .flatten()
        .flatten()
        .any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("rocketmq-filter-")
        })
}

fn tool(name: &str) -> PathBuf {
    if let Ok(home) = env::var("JAVA_HOME") {
        let path = Path::new(&home).join("bin").join(name);
        if path.is_file() {
            return path;
        }
    }
    PathBuf::from(name)
}

fn properties(set: &[(&str, &str)]) -> BTreeMap<String, String> {
    set.iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

/// Operand forms that reach every branch of the original `__compare` alignment table.
const ATOMS: &[&str] = &[
    "level",
    "name",
    "weird",
    "dot",
    "price",
    "flag",
    "up",
    "blank",
    "zero",
    "neg",
    "huge",
    "exp",
    "tags",
    "missing",
    "10",
    "-5",
    "3.5",
    "1E3",
    "0",
    "9007199254740993",
    "2147483648",
    "'10'",
    "'abc'",
    "''",
    "TRUE",
    "FALSE",
    "NULL",
    "1+1",
    "(1)",
    "level+0",
];

const OPERATORS: &[&str] = &["=", "<>", ">", ">=", "<", "<="];

/// Cases as `(property set, expression)` pairs.
fn corpus() -> Vec<(usize, String)> {
    let mut cases = Vec::new();
    for set in 0..PROPERTIES.len() {
        for expression in EXPRESSIONS {
            cases.push((set, (*expression).to_string()));
        }
    }
    // The full operator cross product of every operand pair.
    for operator in OPERATORS {
        for (index, left) in ATOMS.iter().enumerate() {
            for right in ATOMS {
                cases.push((
                    index % PROPERTIES.len(),
                    format!("{left} {operator} {right}"),
                ));
            }
        }
    }
    // BETWEEN validates its bounds while the expression is being built, so vary all three.
    for (index, left) in ATOMS.iter().enumerate() {
        for (step, middle) in ATOMS.iter().enumerate() {
            let right = ATOMS[(index + step) % ATOMS.len()];
            cases.push((
                index % PROPERTIES.len(),
                format!("{left} BETWEEN {middle} AND {right}"),
            ));
        }
    }
    cases
}

fn encode(set: &[(&str, &str)], expression: &str) -> String {
    let encoded = set
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(";");
    let line = format!("{encoded}\t{expression}");
    assert!(
        line.is_ascii(),
        "the oracle protocol is line oriented ascii: {line}"
    );
    line
}

fn rust_verdict(expression: &str, values: &BTreeMap<String, String>) -> String {
    let Ok(compiled) = filter::compile(expression) else {
        return "PARSE_ERROR".into();
    };
    match compiled.evaluate(values) {
        Err(_) => "EVAL_ERROR".into(),
        Ok(Value::Null) => "NULL".into(),
        Ok(Value::Bool(true)) => "TRUE".into(),
        Ok(Value::Bool(false)) => "FALSE".into(),
        Ok(_) => "NON_BOOLEAN".into(),
    }
}

fn java_verdicts(lines: &[String], classpath: &Path, classes: &Path) -> Vec<String> {
    let mut child = Command::new(tool("java"))
        .arg("-cp")
        .arg(format!(
            "{}:{}",
            classes.display(),
            classpath.join("*").display()
        ))
        .arg("SqlFilterDiff")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start the Java filter oracle");
    let mut stdin = child.stdin.take().expect("stdin pipe");
    let input = format!("{}\n", lines.join("\n"));
    let writer = thread::spawn(move || {
        stdin
            .write_all(input.as_bytes())
            .expect("failed to feed the Java filter oracle");
        stdin.flush().expect("failed to close the oracle input");
    });
    let output = child
        .wait_with_output()
        .expect("the Java filter oracle crashed");
    writer.join().expect("the oracle input writer panicked");
    assert!(
        output.status.success(),
        "the Java filter oracle exited with {:?}: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
    );
    let verdicts = String::from_utf8(output.stdout)
        .expect("the oracle emitted non utf8")
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(
        verdicts.len(),
        lines.len(),
        "the oracle answered a different number of cases"
    );
    verdicts
}

#[test]
fn rust_sql92_engine_matches_the_java_parser() {
    let Some(tree) = reference_tree() else {
        eprintln!("skipping: reference tree not found (set ROCKETMQ_JAVA_HOME)");
        return;
    };
    let Some(lib) = distribution_lib(&tree) else {
        eprintln!(
            "skipping: no distribution lib directory under {}",
            tree.display()
        );
        return;
    };
    if !has_jar(&lib) {
        eprintln!("skipping: {} holds no rocketmq-filter jar", lib.display());
        return;
    }

    let classes = Path::new(env!("CARGO_TARGET_TMPDIR")).join("sqlfilter-classes");
    fs::create_dir_all(&classes).expect("failed to create the oracle classes directory");
    let compiled = Command::new(tool("javac"))
        .arg("--release")
        .arg("8")
        .arg("-encoding")
        .arg("UTF-8")
        .arg("-cp")
        .arg(lib.join("*").to_string_lossy().into_owned())
        .arg("-d")
        .arg(&classes)
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sqlfilter/SqlFilterDiff.java"))
        .output()
        .expect("failed to run javac");
    assert!(
        compiled.status.success(),
        "failed to compile the Java oracle: {}",
        String::from_utf8_lossy(&compiled.stderr),
    );

    let corpus = corpus();
    let lines = corpus
        .iter()
        .map(|(set, expression)| encode(PROPERTIES[*set], expression))
        .collect::<Vec<_>>();
    assert!(
        corpus.len() > 2_000,
        "the corpus lost cases: {}",
        corpus.len()
    );

    let verdicts = java_verdicts(&lines, &lib, &classes);
    let mut mismatches = Vec::new();
    for ((set, expression), expected) in corpus.iter().zip(&verdicts) {
        let values = properties(PROPERTIES[*set]);
        let found = rust_verdict(expression, &values);
        if &found != expected {
            mismatches.push(format!(
                "{expression} with {values:?}: java {expected}, rust {found}"
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of {} cases diverged:\n{}",
        mismatches.len(),
        corpus.len(),
        mismatches[..20.min(mismatches.len())].join("\n"),
    );
}
