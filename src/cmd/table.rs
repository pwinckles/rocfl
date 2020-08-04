use core::iter;
use std::{cmp, io};
use std::io::{ErrorKind, Result, Write};

use ansi_term::Style;
use unicode_width::UnicodeWidthStr;

pub trait AsRow<'a> {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a>;
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
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
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum Alignment {
    Left,
    Right,
}

#[derive(Debug)]
pub struct Column {
    pub id: ColumnId,
    heading: String,
    alignment: Alignment,
    width: usize,
}

pub struct Row<'a> {
    cells: Vec<TextCell<'a>>,
}

#[derive(Debug)]
pub struct TextCell<'a> {
    value_owned: Option<String>,
    value_ref: Option<&'a str>,
    width: usize,
    style: Style,
}

pub struct TableView<'a> {
    display_header: bool,
    columns: Vec<Column>,
    rows: Vec<Row<'a>>,
    separator: String,
    enable_styling: bool,
}

impl<'a> TableView<'a> {
    pub fn new(columns: Vec<Column>,
               separator: &str,
               display_header: bool,
               enable_styling: bool) -> Self {
        let mut table = Self {
            display_header,
            columns,
            rows: Vec::new(),
            separator: separator.to_owned(),
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
        if self.display_header {
            self.write_header(writer)?;
        }

        for row in self.rows.iter() {
            row.write(writer, &self.columns, &self.separator, self.enable_styling)?;
        }

        Ok(())
    }

    fn write_header(&self, writer: &mut impl Write) -> Result<()> {
        let iter = &mut self.columns.iter();
        let mut next = iter.next();

        while let Some(column) = next {
            next = iter.next();

            let width = if next.is_some() { column.width } else { 0 };

            column.heading_cell().write(writer, width, Alignment::Left, self.enable_styling)?;

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
        let mut cell = TextCell::new_ref(&self.heading);
        cell.style = Style::new().underline();
        cell
    }
}

impl<'a> Row<'a> {
    pub fn new(cells: Vec<TextCell<'a>>) -> Self {
        Row {
            cells
        }
    }

    fn write(&self, writer: &mut impl Write,
             columns: &[Column],
             separator: &str,
             enable_styling: bool) -> Result<()> {
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
                write!(writer, "{}", &separator)?;
            }
        }

        writeln!(writer)
    }
}

impl<'a> TextCell<'a> {
    pub fn new_owned(value: &str) -> Self {
        Self {
            width: UnicodeWidthStr::width(value),
            value_owned: Some(value.to_owned()),
            value_ref: None,
            style: Style::default(),
        }
    }

    pub fn new_ref(value: &'a str) -> Self {
        Self {
            width: UnicodeWidthStr::width(value),
            value_owned: None,
            value_ref: Some(value),
            style: Style::default(),
        }
    }

    pub fn blank() -> Self {
        Self::new_owned("")
    }

    pub fn with_style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }

    fn width(&self) -> usize {
        self.width
    }

    fn value(&self) -> &str {
        if let Some(owned) = &self.value_owned {
            owned
        } else {
            self.value_ref.unwrap()
        }
    }

    fn write(&self, writer: &mut impl Write,
             width: usize,
             alignment: Alignment,
             enable_style: bool) -> Result<()> {

        let spaces: String = if width == 0 {
            "".to_owned()
        } else {
            iter::repeat(' ').take(width - self.width).collect()
        };

        let style = if enable_style {
            self.style
        } else {
            Style::default()
        };

        match alignment {
            Alignment::Left => write!(writer, "{}{}", style.paint(self.value()), spaces),
            Alignment::Right => write!(writer, "{}{}", spaces, style.paint(self.value()))
        }
    }
}