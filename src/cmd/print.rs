use std::cmp;

use unicode_width::UnicodeWidthStr;

pub trait AsRow<'a> {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a>;
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum ColumnId {
    Version,
    Updated,
    ObjectId,
    LogicalPath,
    PhysicalPath,
    Digest,
}

#[derive(Debug)]
pub struct Column {
    pub id: ColumnId,
    heading: String,
    alignment: Alignment,
    width: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum Alignment {
    Left,
    Right,
}

pub struct Row<'a> {
    cells: Vec<TextCell<'a>>,
}

#[derive(Debug)]
pub struct TextCell<'a> {
    value_owned: Option<String>,
    value_ref: Option<&'a str>,
    width: usize,
}

pub struct TableView<'a> {
    display_header: bool,
    columns: Vec<Column>,
    rows: Vec<Row<'a>>,
}

impl<'a> TableView<'a> {
    pub fn new(columns: Vec<Column>, display_header: bool) -> Self {
        let mut table = Self {
            display_header,
            columns,
            rows: Vec::new(),
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

    pub fn write(&self) {
        if self.display_header {
            self.write_header();
        }
        self.rows.iter().for_each(|row| row.write(&self.columns));
    }

    fn write_header(&self) {
        let iter = &mut self.columns.iter();
        let mut next = iter.next();

        while let Some(column) = next {
            column.as_cell().write(column.width, Alignment::Left);
            next = iter.next();
            if next.is_some() {
                // TODO
                print!(" ")
            }
        }

        println!()
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

    fn as_cell(&self) -> TextCell {
        TextCell::new_ref(&self.heading)
    }
}

impl<'a> Row<'a> {
    pub fn new(cells: Vec<TextCell<'a>>) -> Self {
        Row {
            cells
        }
    }

    fn write(&self, columns: &[Column]) {
        let mut iter = self.cells.iter().zip(columns);
        let mut next = iter.next();

        while let Some((cell, column)) = next {
            cell.write(column.width, column.alignment);
            next = iter.next();
            if next.is_some() {
                // TODO
                print!(" ")
            }
        }

        println!()
    }
}

impl<'a> TextCell<'a> {
    pub fn new_owned(value: &str) -> Self {
        Self {
            width: UnicodeWidthStr::width(value),
            value_owned: Some(value.to_owned()),
            value_ref: None,
        }
    }

    pub fn new_ref(value: &'a str) -> Self {
        Self {
            width: UnicodeWidthStr::width(value),
            value_owned: None,
            value_ref: Some(value),
        }
    }

    pub fn blank() -> Self {
        Self::new_owned("")
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

    fn write(&self, width: usize, alignment: Alignment) {
        // TODO change to write
        match alignment {
            Alignment::Left => print!("{:<width$}", self.value(), width = width),
            Alignment::Right => print!("{:>width$}", self.value(), width = width)
        }
    }
}