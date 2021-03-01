use ansi_term::{Color, Style};
use once_cell::sync::Lazy;

pub static DEFAULT: Lazy<Style> = Lazy::new(Style::default);

pub static GREEN: Lazy<Style> = Lazy::new(|| Style::new().fg(Color::Green));
pub static RED: Lazy<Style> = Lazy::new(|| Style::new().fg(Color::Red));
pub static CYAN: Lazy<Style> = Lazy::new(|| Style::new().fg(Color::Cyan));
pub static YELLOW: Lazy<Style> = Lazy::new(|| Style::new().fg(Color::Yellow));

pub static UNDERLINE: Lazy<Style> = Lazy::new(|| Style::new().underline());
pub static BOLD: Lazy<Style> = Lazy::new(|| Style::new().bold());
