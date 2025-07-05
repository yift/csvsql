use std::io::{self, BufRead};
use std::io::{IsTerminal, Write};

use itertools::Itertools;
use reedline::{
    ColumnarMenu, DefaultCompleter, DefaultPrompt, DefaultPromptSegment, Emacs, ExampleHighlighter,
    FileBackedHistory, KeyCode, KeyModifiers, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu,
    Signal, ValidationResult, Validator, default_emacs_keybindings,
};

use crate::engine::Engine;
use crate::error::CvsSqlError;
use crate::outputer::Outputer;

struct EolValidator {}
impl Validator for EolValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        if line.ends_with("\\") {
            ValidationResult::Incomplete
        } else {
            ValidationResult::Complete
        }
    }
}
pub fn work_on_console(
    engine: &Engine,
    no_console: bool,
    outputer: &mut dyn Outputer,
) -> Result<(), CvsSqlError> {
    if io::stdout().is_terminal()
        && io::stdin().is_terminal()
        && io::stderr().is_terminal()
        && !no_console
    {
        use_readline(engine, outputer)
    } else {
        stdout(engine, outputer)
    }
}
struct ReadlineRepl {
    editor: Reedline,
}
impl ReplOutputer for ReadlineRepl {
    fn get_commands(&mut self, prompt: &str) -> Result<Option<String>, CvsSqlError> {
        let left_prompt = DefaultPromptSegment::Basic(prompt.to_string());
        let right_prompt = DefaultPromptSegment::Empty;
        let prompt = DefaultPrompt::new(left_prompt, right_prompt);
        let sig = self.editor.read_line(&prompt)?;
        match sig {
            Signal::Success(command) => Ok(Some(command)),
            Signal::CtrlD | Signal::CtrlC => Ok(None),
        }
    }
}
fn use_readline(engine: &Engine, outputer: &mut dyn Outputer) -> Result<(), CvsSqlError> {
    let mut line_editor = Reedline::create();
    if let Some(config_dir) = dirs::config_dir() {
        let history = config_dir.join("csvsql").join(".history");
        if let Ok(history) = FileBackedHistory::with_file(150, history) {
            line_editor = line_editor.with_history(Box::new(history));
        }
    };
    let commands_to_complete = vec![
        "SELECT".into(),
        "UPDATE".into(),
        "DELETE".into(),
        "INSERT".into(),
        "FROM".into(),
        "WHERE".into(),
        "GROUP BY".into(),
        "HAVING".into(),
        "LIMIT".into(),
        "CREATE".into(),
        "DROP".into(),
        "TEMPORARY".into(),
    ];
    let completer = DefaultCompleter::new_with_wordlen(commands_to_complete, 2);
    let completion_menu = Box::new(ColumnarMenu::default().with_name("completion_menu"));
    let mut keybindings = default_emacs_keybindings();
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu("completion_menu".to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );

    let commands_to_highlight = vec![
        "SELECT".into(),
        "UPDATE".into(),
        "DELETE".into(),
        "INSERT".into(),
        "CREATE".into(),
        "DROP".into(),
    ];
    let highlighter = Box::new(ExampleHighlighter::new(commands_to_highlight));

    let edit_mode = Box::new(Emacs::new(keybindings));
    line_editor = line_editor
        .with_completer(Box::new(completer))
        .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
        .with_edit_mode(edit_mode)
        .with_validator(Box::new(EolValidator {}))
        .with_highlighter(highlighter);
    let mut reeline = ReadlineRepl {
        editor: line_editor,
    };
    repl(engine, outputer, &mut reeline)
}

trait ReplOutputer {
    fn get_commands(&mut self, prompt: &str) -> Result<Option<String>, CvsSqlError>;
    fn print_error(&self, err: CvsSqlError) {
        eprintln!("Got error: {err}")
    }
    fn print_output(&self, output: &str) {
        println!("{output}")
    }
}
struct SimpleStdRepl {}
impl ReplOutputer for SimpleStdRepl {
    fn get_commands(&mut self, prompt: &str) -> Result<Option<String>, CvsSqlError> {
        let mut stdout = io::stdout().lock();
        print!("{prompt} > ");
        stdout.flush()?;

        let stdin = io::stdin();
        let validator = EolValidator {};
        let mut command_to_execute = vec![];
        while let Some(line) = stdin.lock().lines().next() {
            let command = line?;
            command_to_execute.push(command.to_string());
            if let ValidationResult::Complete = validator.validate(&command) {
                break;
            }
        }

        if command_to_execute.is_empty() {
            Ok(None)
        } else {
            let command = command_to_execute.iter().join("\n");
            Ok(Some(command))
        }
    }
}

fn stdout(engine: &Engine, outputer: &mut dyn Outputer) -> Result<(), CvsSqlError> {
    let mut std = SimpleStdRepl {};
    repl(engine, outputer, &mut std)
}
fn repl(
    engine: &Engine,
    outputer: &mut dyn Outputer,
    repl: &mut impl ReplOutputer,
) -> Result<(), CvsSqlError> {
    loop {
        match repl.get_commands(&engine.prompt())? {
            None => {
                return Ok(());
            }
            Some(command) => {
                let command = command.replace("\\\n", "\n");

                match engine.execute_commands(&command) {
                    Ok(results) => {
                        for results in results {
                            if let Some(out) = outputer.write(&results)? {
                                repl.print_output(&out);
                            }
                        }
                    }
                    Err(e) => repl.print_error(e),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::cell::RefCell;

    use crate::args::Args;

    use super::*;

    struct TestRepl {
        prompts: Vec<String>,
        errors: RefCell<Vec<CvsSqlError>>,
        outputs: RefCell<Vec<String>>,
        index: usize,
        inputs: Vec<String>,
    }
    impl TestRepl {
        fn new(inputs: Vec<String>) -> Self {
            TestRepl {
                prompts: vec![],
                errors: RefCell::new(vec![]),
                outputs: RefCell::new(vec![]),
                index: 0,
                inputs,
            }
        }
    }
    #[derive(Default)]
    struct TestOutputer {
        results_count: usize,
        results: Vec<String>,
    }
    impl TestOutputer {
        fn new(results: Vec<String>) -> Self {
            Self {
                results_count: 0,
                results,
            }
        }
    }
    impl Outputer for TestOutputer {
        fn write(
            &mut self,
            _: &crate::engine::CommandExecution,
        ) -> Result<Option<String>, CvsSqlError> {
            let ret = self.results.get(self.results_count).cloned();
            self.results_count += 1;
            Ok(ret)
        }
    }

    impl ReplOutputer for TestRepl {
        fn get_commands(&mut self, prompt: &str) -> Result<Option<String>, CvsSqlError> {
            self.prompts.push(prompt.to_string());
            match self.inputs.get(self.index) {
                None => Ok(None),
                Some(str) => {
                    self.index += 1;
                    Ok(Some(str.to_string()))
                }
            }
        }
        fn print_error(&self, err: CvsSqlError) {
            self.errors.borrow_mut().push(err);
        }
        fn print_output(&self, output: &str) {
            self.outputs.borrow_mut().push(output.to_string());
        }
    }

    #[test]
    fn test_repl() -> Result<(), CvsSqlError> {
        let args = Args::default();
        let engine = Engine::try_from(&args)?;
        let mut outputer = TestOutputer::new(vec!["one".into(), "two".into()]);
        let mut test_repl = TestRepl::new(vec![
            "SELECT * FROM \\\n tests.data.artists".into(),
            "SELECT;".into(),
            "START TRANSACTION".into(),
            "ROLLBACK".into(),
        ]);

        repl(&engine, &mut outputer, &mut test_repl)?;

        assert_eq!(outputer.results_count, 3);
        assert_eq!(test_repl.errors.borrow().len(), 1);
        assert_eq!(test_repl.prompts.len(), 5);
        assert_eq!(test_repl.errors.borrow().len(), 1);
        assert_eq!(test_repl.outputs.borrow().len(), 2);

        Ok(())
    }
}
