use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::Terminal;

use crate::stats::{ConnState, SharedStats};

/// Run the interactive terminal dashboard. Blocks until the user presses `q`
/// or Ctrl-C, redrawing at ~4Hz from the shared stats snapshot.
pub async fn run(stats: SharedStats) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = event_loop(&mut terminal, stats).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    stats: SharedStats,
) -> anyhow::Result<()> {
    loop {
        let snapshot = {
            let s = stats.read().await;
            (
                s.started_at,
                s.streams.values().cloned().collect::<Vec<_>>(),
                s.indices_db.clone(),
                s.options_db.clone(),
            )
        };

        terminal.draw(|f| draw(f, snapshot))?;

        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                    break;
                }
            }
        }
    }

    Ok(())
}

type Snapshot = (
    Option<chrono::DateTime<chrono::Local>>,
    Vec<crate::stats::StreamStat>,
    crate::stats::DbStat,
    crate::stats::DbStat,
);

fn draw(f: &mut ratatui::Frame, snapshot: Snapshot) {
    let (started_at, mut streams, indices_db, options_db) = snapshot;
    streams.sort_by(|a, b| a.name.cmp(&b.name));

    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(6),
            Constraint::Length(1),
        ])
        .split(area);

    draw_header(f, chunks[0], started_at);
    draw_streams_table(f, chunks[1], &streams);
    draw_db_panel(f, chunks[2], &indices_db, &options_db);
    draw_footer(f, chunks[3]);
}

fn draw_header(f: &mut ratatui::Frame, area: Rect, started_at: Option<chrono::DateTime<chrono::Local>>) {
    let uptime = started_at
        .map(|s| {
            let dur = chrono::Local::now().signed_duration_since(s);
            format!(
                "{:02}:{:02}:{:02}",
                dur.num_hours(),
                dur.num_minutes() % 60,
                dur.num_seconds() % 60
            )
        })
        .unwrap_or_else(|| "--:--:--".to_string());

    let text = Line::from(vec![
        Span::styled(" kstocks-server ", Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan)),
        Span::raw(" | market data collector | uptime "),
        Span::styled(uptime, Style::default().fg(Color::Yellow)),
    ]);

    let block = Block::default().borders(Borders::ALL).title(" Control Panel ");
    f.render_widget(Paragraph::new(text).block(block), area);
}

fn draw_streams_table(f: &mut ratatui::Frame, area: Rect, streams: &[crate::stats::StreamStat]) {
    let header = Row::new(vec![
        Cell::from("Stream"),
        Cell::from("State"),
        Cell::from("Ticks"),
        Cell::from("Last Tick"),
        Cell::from("Reconnects"),
        Cell::from("Last Error"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = streams
        .iter()
        .map(|s| {
            let state_style = match s.state {
                ConnState::Connected => Style::default().fg(Color::Green),
                ConnState::Connecting => Style::default().fg(Color::Yellow),
                ConnState::Reconnecting => Style::default().fg(Color::Red),
                ConnState::Stopped => Style::default().fg(Color::DarkGray),
            };
            let last_tick = s
                .last_tick_at
                .map(|t| t.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "-".to_string());
            let err = s.last_error.clone().unwrap_or_default();

            Row::new(vec![
                Cell::from(s.name.clone()),
                Cell::from(Span::styled(s.state.label(), state_style)),
                Cell::from(s.ticks_received.to_string()),
                Cell::from(last_tick),
                Cell::from(s.reconnect_count.to_string()),
                Cell::from(err),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(28),
        Constraint::Length(13),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(11),
        Constraint::Min(20),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" WebSocket Streams (6) "));

    f.render_widget(table, area);
}

fn draw_db_panel(
    f: &mut ratatui::Frame,
    area: Rect,
    indices_db: &crate::stats::DbStat,
    options_db: &crate::stats::DbStat,
) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    render_db_stat(f, chunks[0], "Index Ticks DB", indices_db);
    render_db_stat(f, chunks[1], "Option Ticks DB", options_db);
}

fn render_db_stat(f: &mut ratatui::Frame, area: Rect, title: &str, stat: &crate::stats::DbStat) {
    let last_flush = stat
        .last_flush_at
        .map(|t| t.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "-".to_string());
    let err = stat.last_error.clone().unwrap_or_else(|| "-".to_string());

    let lines = vec![
        Line::from(format!("Rows written:   {}", stat.rows_written)),
        Line::from(format!("Rows pending:   {} (in-memory buffer)", stat.rows_pending)),
        Line::from(format!("Last flush:     {} ({} rows)", last_flush, stat.last_flush_rows)),
        Line::from(vec![
            Span::raw("Last error:     "),
            Span::styled(err, Style::default().fg(Color::Red)),
        ]),
    ];

    let block = Block::default().borders(Borders::ALL).title(format!(" {} ", title));
    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn draw_footer(f: &mut ratatui::Frame, area: Rect) {
    let text = Line::from(Span::styled(
        " Press q or Esc to quit (streaming continues in background) ",
        Style::default().fg(Color::DarkGray),
    ));
    f.render_widget(Paragraph::new(text), area);
}
