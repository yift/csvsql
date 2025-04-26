use std::io::IsTerminal;
use std::io::{self, BufRead, Write};

use reedline::{
    ColumnarMenu, DefaultCompleter, DefaultPrompt, DefaultPromptSegment, Emacs, ExampleHighlighter,
    FileBackedHistory, KeyCode, KeyModifiers, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu,
    Signal, default_emacs_keybindings,
};

use crate::engine::Engine;
use crate::error::CvsSqlError;
use crate::outputer::Outputer;

pub fn work_on_console(
    engine: &Engine,
    no_console: bool,
    outputer: Box<dyn Outputer>,
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
fn use_readline(engine: &Engine, mut outputer: Box<dyn Outputer>) -> Result<(), CvsSqlError> {
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
        .with_highlighter(highlighter);

    loop {
        let left_prompt = DefaultPromptSegment::Basic(engine.prompt());
        let right_prompt = DefaultPromptSegment::Empty;
        let prompt = DefaultPrompt::new(left_prompt, right_prompt);
        let sig = line_editor.read_line(&prompt)?;
        match sig {
            Signal::Success(command) => match engine.execute_commands(&command) {
                Err(err) => println!("Gotr error: {}", err),
                Ok(results) => {
                    for results in results {
                        if let Some(out) = outputer.write(&results)? {
                            println!("{}", out);
                        }
                    }
                }
            },
            Signal::CtrlD | Signal::CtrlC => return Ok(()),
        }
    }
}

fn stdout(engine: &Engine, mut outputer: Box<dyn Outputer>) -> Result<(), CvsSqlError> {
    let stdin = io::stdin();
    loop {
        let mut stdout = io::stdout().lock();
        print!("{} >", engine.prompt());
        stdout.flush()?;

        if let Some(line) = stdin.lock().lines().next() {
            let command = line?;

            for results in engine.execute_commands(&command)? {
                if let Some(out) = outputer.write(&results)? {
                    println!("{}", out);
                }
            }
        } else {
            return Ok(());
        }
    }
}
