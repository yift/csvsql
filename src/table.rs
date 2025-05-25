use std::io;

use crossterm::event::{self, Event};
use ratatui::{Frame, Terminal, prelude::Backend};

use crate::{error::CvsSqlError, results::ResultSet};

use ratatui::{
    crossterm::event::{KeyCode, KeyEventKind},
    layout::{Constraint, Layout, Margin, Rect},
    style::{self, Color, Modifier, Style, Stylize},
    text::Text,
    widgets::{
        Cell, HighlightSpacing, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState,
        Table, TableState,
    },
};
use style::palette::tailwind;
use unicode_width::UnicodeWidthStr;

const PALETTE: tailwind::Palette = tailwind::SKY;
const INFO_TEXT: &str = "(Esc) quit | (↑) move up | (↓) move down";

pub(crate) fn draw_table(results: &ResultSet) -> Result<(), CvsSqlError> {
    let mut terminal = ratatui::init();
    let result = TableApp::new(results, event::read).run(&mut terminal);
    ratatui::restore();
    result
}

// TO review

struct TableColors {
    buffer_bg: Color,
    header_bg: Color,
    header_fg: Color,
    row_fg: Color,
    selected_row_style_fg: Color,
    selected_column_style_fg: Color,
    selected_cell_style_fg: Color,
    normal_row_color: Color,
    alt_row_color: Color,
}

impl TableColors {
    const fn new(color: &tailwind::Palette) -> Self {
        Self {
            buffer_bg: color.c50,
            header_bg: color.c200,
            header_fg: color.c900,
            row_fg: color.c800,
            selected_row_style_fg: color.c900,
            selected_column_style_fg: color.c900,
            selected_cell_style_fg: color.c500,
            normal_row_color: color.c50,
            alt_row_color: color.c100,
        }
    }
}

struct TableApp {
    state: TableState,
    headers: Vec<String>,
    constraints: Vec<Constraint>,
    data: Vec<Vec<String>>,
    scroll_state: ScrollbarState,
    colors: TableColors,
    next_event: fn() -> io::Result<Event>,
}

impl TableApp {
    fn new(results: &ResultSet, next_event: fn() -> io::Result<Event>) -> Self {
        let mut headers = vec![];
        let mut longest_item_lens = vec![];
        for col in results.columns() {
            let name = results.metadata.column_title(&col).to_string();
            let width = UnicodeWidthStr::width(name.as_str());
            longest_item_lens.push(width);
            headers.push(name);
        }
        let mut data = vec![];
        for row in results.data.iter() {
            let mut line = vec![];
            for col in results.columns() {
                let val = row.get(&col).to_string();
                let width = UnicodeWidthStr::width(val.as_str());
                if longest_item_lens[col.get_index()] < width {
                    longest_item_lens[col.get_index()] = width;
                }
                line.push(val);
            }
            data.push(line);
        }
        let mut constraints = vec![];
        for (i, l) in longest_item_lens.iter().enumerate() {
            let l = l + 1;
            if i == 0 {
                constraints.push(Constraint::Length(l as u16));
            } else {
                constraints.push(Constraint::Min(l as u16));
            }
        }
        Self {
            state: TableState::default().with_selected(0),
            constraints,
            scroll_state: ScrollbarState::new(data.len() - 1),
            colors: TableColors::new(&PALETTE),
            data,
            headers,
            next_event,
        }
    }
    pub fn next_row(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i >= self.data.len() - 1 {
                    i
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
        self.scroll_state = self.scroll_state.position(i);
    }
    pub fn next_rows(&mut self) {
        let i = match self.state.selected() {
            Some(i) => i + 20,
            None => 20,
        };
        let i = if i >= self.data.len() {
            self.data.len() - 1
        } else {
            i
        };
        self.state.select(Some(i));
        self.scroll_state = self.scroll_state.position(i);
    }

    pub fn previous_rows(&mut self) {
        let i = match self.state.selected() {
            Some(i) => i.saturating_sub(20),
            None => 0,
        };
        self.state.select(Some(i));
        self.scroll_state = self.scroll_state.position(i);
    }

    pub fn home(&mut self) {
        if !self.data.is_empty() {
            self.state.select(Some(0));
        } else {
            self.state.select(None);
        }
        self.scroll_state = self.scroll_state.position(0);
    }

    pub fn end(&mut self) {
        if !self.data.is_empty() {
            self.state.select(Some(self.data.len() - 1));
            self.scroll_state = self.scroll_state.position(self.data.len() - 1);
        } else {
            self.state.select(None);
            self.scroll_state = self.scroll_state.position(0);
        }
    }

    pub fn previous_row(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i == 0 {
                    0
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
        self.scroll_state = self.scroll_state.position(i);
    }

    pub fn next_column(&mut self) {
        self.state.select_next_column();
    }

    pub fn previous_column(&mut self) {
        self.state.select_previous_column();
    }

    fn draw_on_term<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<(), CvsSqlError> {
        terminal.draw(|frame| self.draw(frame))?;
        Ok(())
    }

    fn run<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<(), CvsSqlError> {
        loop {
            self.draw_on_term(terminal)?;

            if let Event::Key(key) = (self.next_event)()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                        KeyCode::Char('j') | KeyCode::Down => self.next_row(),
                        KeyCode::Char('k') | KeyCode::Up => self.previous_row(),
                        KeyCode::Char('l') | KeyCode::Right => self.next_column(),
                        KeyCode::Char('h') | KeyCode::Left => self.previous_column(),
                        KeyCode::PageDown => self.next_rows(),
                        KeyCode::PageUp => self.previous_rows(),
                        KeyCode::End => self.end(),
                        KeyCode::Home => self.home(),
                        _ => {}
                    }
                }
            }
        }
    }

    fn draw(&mut self, frame: &mut Frame) {
        let vertical = &Layout::vertical([Constraint::Min(5), Constraint::Length(1)]);
        let rects = vertical.split(frame.area());

        self.colors = TableColors::new(&PALETTE);

        self.render_table(frame, rects[0]);
        self.render_scrollbar(frame, rects[0]);
        self.render_footer(frame, rects[1]);
    }

    fn render_table(&mut self, frame: &mut Frame, area: Rect) {
        let header_style = Style::default()
            .fg(self.colors.header_fg)
            .bg(self.colors.header_bg);
        let selected_row_style = Style::default()
            .add_modifier(Modifier::REVERSED)
            .fg(self.colors.selected_row_style_fg);
        let selected_col_style = Style::default().fg(self.colors.selected_column_style_fg);
        let selected_cell_style = Style::default()
            .add_modifier(Modifier::REVERSED)
            .fg(self.colors.selected_cell_style_fg);

        let header = self
            .headers
            .clone()
            .into_iter()
            .map(Cell::from)
            .collect::<Row>()
            .style(header_style)
            .height(1);
        let rows = self.data.iter().enumerate().map(|(i, data)| {
            let color = match i % 2 {
                0 => self.colors.normal_row_color,
                _ => self.colors.alt_row_color,
            };
            data.iter()
                .map(|content| Cell::from(Text::from(content.to_string())))
                .collect::<Row>()
                .style(Style::new().fg(self.colors.row_fg).bg(color))
                .height(1)
        });
        let bar = " █ ";
        let t = Table::new(rows, &self.constraints)
            .header(header)
            .row_highlight_style(selected_row_style)
            .column_highlight_style(selected_col_style)
            .cell_highlight_style(selected_cell_style)
            .highlight_symbol(Text::from(vec![
                "".into(),
                bar.into(),
                bar.into(),
                "".into(),
            ]))
            .bg(self.colors.buffer_bg)
            .highlight_spacing(HighlightSpacing::Always);
        frame.render_stateful_widget(t, area, &mut self.state);
    }

    fn render_scrollbar(&mut self, frame: &mut Frame, area: Rect) {
        frame.render_stateful_widget(
            Scrollbar::default()
                .orientation(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None),
            area.inner(Margin {
                vertical: 1,
                horizontal: 1,
            }),
            &mut self.scroll_state,
        );
    }

    fn render_footer(&self, frame: &mut Frame, area: Rect) {
        let info_footer = Paragraph::new(Text::from(INFO_TEXT))
            .style(
                Style::new()
                    .fg(self.colors.row_fg)
                    .bg(self.colors.buffer_bg),
            )
            .centered();
        frame.render_widget(info_footer, area);
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
    use insta::assert_snapshot;
    use ratatui::{Terminal, backend::TestBackend};

    use crate::{args::Args, engine::Engine, error::CvsSqlError};

    use super::TableApp;

    fn send_q() -> std::io::Result<Event> {
        let event = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::empty());
        Ok(Event::Key(event))
    }
    #[test]
    fn test_draw_table() -> Result<(), CvsSqlError> {
        let args = Args::default();
        let engine = Engine::try_from(&args)?;

        let results = engine.execute_commands("SELECT * FROM tests.data.sales")?;
        let results = &results.first().unwrap().results;

        let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();

        let mut table = TableApp::new(results, send_q);
        table.run(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.next_row();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.next_row();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.previous_row();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.previous_row();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.previous_row();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.previous_column();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.next_column();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.next_column();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.previous_column();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.previous_rows();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.next_rows();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.next_rows();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.previous_rows();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.end();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.next_row();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.next_rows();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        table.home();
        table.draw_on_term(&mut terminal)?;
        assert_snapshot!(terminal.backend());

        Ok(())
    }
}
