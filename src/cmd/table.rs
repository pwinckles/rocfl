use std::fmt::{self, Display, Formatter};
use std::io::{ErrorKind, Result, Write};
use std::{cmp, io};

use ansi_term::Style;
use unicode_width::UnicodeWidthStr;

use crate::cmd::style;

pub trait AsRow<'a> {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a>;
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum ColumnId {
    Version,
    Created,
    ObjectId,
    LogicalPath,
    PhysicalPath,
    Digest,
    Author,
    Address,
    Message,
    Operation,
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum Alignment {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Separator {
    Tab,
    Space,
}

pub struct Column {
    pub id: ColumnId,
    heading: String,
    alignment: Alignment,
    width: usize,
}

pub struct Row<'a> {
    cells: Vec<TextCell<'a>>,
}

pub struct TextCell<'a> {
    value: Box<dyn AsRef<str> + 'a>,
    width: usize,
    style: &'static Style,
}

pub struct TableView<'a> {
    display_header: bool,
    columns: Vec<Column>,
    rows: Vec<Row<'a>>,
    separator: Separator,
    enable_styling: bool,
}

impl<'a> TableView<'a> {
    pub fn new(
        columns: Vec<Column>,
        separator: Separator,
        display_header: bool,
        enable_styling: bool,
    ) -> Self {
        let mut table = Self {
            display_header,
            columns,
            rows: Vec::new(),
            separator,
            enable_styling,
        };

        if display_header {
            table.add_heading_widths();
        }

        table
    }

    pub fn add_row(&mut self, row: &'a impl AsRow<'a>) {
        let row = row.as_row(&self.columns);
        for (column, cell) in self.columns.iter_mut().zip(&row.cells) {
            column.update_width(cell.width());
        }
        self.rows.push(row);
    }

    pub fn write_stdio(&self) -> Result<()> {
        let mut writer = io::stdout();
        writer.lock();
        if let Err(e) = self.write(&mut writer) {
            match e.kind() {
                ErrorKind::BrokenPipe => Ok(()),
                _ => Err(e),
            }
        } else {
            Ok(())
        }
    }

    pub fn write(&self, writer: &mut impl Write) -> Result<()> {
        if self.display_header && !self.rows.is_empty() {
            self.write_header(writer)?;
        }

        for row in &self.rows {
            row.write(writer, &self.columns, self.separator, self.enable_styling)?;
        }

        Ok(())
    }

    fn write_header(&self, writer: &mut impl Write) -> Result<()> {
        let iter = &mut self.columns.iter();
        let mut next = iter.next();

        while let Some(column) = next {
            next = iter.next();

            let width = if next.is_some() { column.width } else { 0 };

            column
                .heading_cell()
                .write(writer, width, Alignment::Left, self.enable_styling)?;

            if next.is_some() {
                write!(writer, "{}", self.separator)?;
            }
        }

        writeln!(writer)
    }

    fn add_heading_widths(&mut self) {
        for column in self.columns.iter_mut() {
            column.update_width(UnicodeWidthStr::width(column.heading.as_str()));
        }
    }
}

impl Column {
    pub fn new(id: ColumnId, heading: &str, alignment: Alignment) -> Self {
        Self {
            id,
            heading: heading.to_owned(),
            alignment,
            width: 0,
        }
    }

    fn update_width(&mut self, new_width: usize) {
        self.width = cmp::max(self.width, new_width);
    }

    fn heading_cell(&self) -> TextCell {
        let mut cell = TextCell::new(&self.heading);
        cell.style = &*style::UNDERLINE;
        cell
    }
}

impl<'a> Row<'a> {
    pub fn new(cells: Vec<TextCell<'a>>) -> Self {
        Row { cells }
    }

    fn write(
        &self,
        writer: &mut impl Write,
        columns: &[Column],
        separator: Separator,
        enable_styling: bool,
    ) -> Result<()> {
        let mut iter = self.cells.iter().zip(columns);
        let mut next = iter.next();

        while let Some((cell, column)) = next {
            next = iter.next();

            let width = if next.is_some() || column.alignment == Alignment::Right {
                column.width
            } else {
                0
            };

            cell.write(writer, width, column.alignment, enable_styling)?;

            if next.is_some() {
                write!(writer, "{}", separator)?;
            }
        }

        writeln!(writer)
    }
}

impl<'a> TextCell<'a> {
    pub fn new(value: impl AsRef<str> + 'a) -> Self {
        Self {
            width: UnicodeWidthStr::width(value.as_ref()),
            value: Box::new(value),
            style: &*style::DEFAULT,
        }
    }

    pub fn blank() -> Self {
        Self::new("")
    }

    pub fn with_style(mut self, style: &'static Style) -> Self {
        self.style = style;
        self
    }

    fn width(&self) -> usize {
        self.width
    }

    fn write(
        &self,
        writer: &mut impl Write,
        width: usize,
        alignment: Alignment,
        enable_style: bool,
    ) -> Result<()> {
        let spaces: String = if width == 0 {
            "".to_owned()
        } else {
            " ".repeat(width - self.width)
        };

        let style = if enable_style {
            self.style
        } else {
            &*style::DEFAULT
        };

        let value = self.value.as_ref().as_ref();

        match alignment {
            Alignment::Left => write!(writer, "{}{}", style.paint(value), spaces),
            Alignment::Right => write!(writer, "{}{}", spaces, style.paint(value)),
        }
    }
}

impl Display for Separator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Separator::Tab => write!(f, "\t"),
            Separator::Space => write!(f, " "),
        }
    }
}
