use ansi_term::{Color, Style};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref DEFAULT: Style = Style::default();

    pub static ref GREEN: Style = Style::new().fg(Color::Green);
    pub static ref RED: Style = Style::new().fg(Color::Red);
    pub static ref CYAN: Style = Style::new().fg(Color::Cyan);
    pub static ref YELLOW: Style = Style::new().fg(Color::Yellow);

    pub static ref UNDERLINE: Style = Style::new().underline();
    pub static ref BOLD: Style = Style::new().bold();
}