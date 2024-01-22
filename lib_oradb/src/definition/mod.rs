/*-
 * SPDX-License-Identifier: BSD-2-Clause-FreeBSD
 *
 * Copyright (c) 2023 Christian Moerz. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
//!
//! Table, column, data type definitions
//!

use std::collections::BTreeMap;

mod builder;
mod meta;
mod oracle;
use crate::Result;
use chrono::{DateTime, Utc};
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};

pub use self::builder::TableSelectionBuilder;
use self::meta::{DataRowProvider, ThreadedDataRowProvider};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

///
/// Available column data type
#[derive(Debug)]
pub enum DataType {
    VarChar(u32),
    Number(u32, u32),
    Boolean,
    Date,
    CLob,
    DateTime,
}

///
/// Defines a table column
#[derive(Debug)]
pub struct ColumnDefinition {
    column_name: String,
    nullable: bool,
    data_type: DataType,
}

///
/// Defines a table
#[derive(Debug)]
pub struct TableDefinition {
    /// table name
    table_name: String,
    /// maps column name to column definition
    columns: BTreeMap<String, ColumnDefinition>,
}

///
/// Defines a row's column value
#[derive(Debug)]
pub enum ColumnValue {
    Varchar(String),
    Float(f64),
    Number(i64),
    Boolean(bool),
    Date(DateTime<Utc>),
    DateTime(DateTime<Utc>),
}

///
/// An indicator for whether there is
/// more data coming or if end of
/// data has been reached.
pub enum RowIndicator {
    EndOfData,
    MoreToCome(Vec<Option<ColumnValue>>),
}

///
/// Describes a data row in a table
#[derive(Debug)]
pub struct DataRow {
    /// back link to column definitions
    column_defs: Rc<BTreeMap<String, ColumnDefinition>>,
    column_values: Vec<Option<ColumnValue>>,
}

///
/// Represents actual table data
#[derive(Debug)]
pub struct TableData {
    /// table name
    table_name: String,
    /// maps column names to definitions
    column_defs: Rc<BTreeMap<String, ColumnDefinition>>,
    /// row data
    data: Vec<DataRow>,
}

///
/// Represents table data that is loaded
/// asynchronously and not collected by the object itself.
/// This permits working with received data while
/// it is still being loaded.
pub struct ThreadedTableData {
    table_name: String,
    /// maps column names to definitions
    column_defs: Rc<BTreeMap<String, ColumnDefinition>>,
    pipe: Arc<RwLock<VecDeque<RowIndicator>>>,
}

impl ThreadedTableData {
    ///
    /// Gets table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Gets iterator over column definitions
    pub fn column_defs(
        &self,
    ) -> std::collections::btree_map::Values<'_, std::string::String, ColumnDefinition> {
        self.column_defs.values()
    }
    /// Get access to data pipe
    pub fn pipe(&self) -> Arc<RwLock<VecDeque<RowIndicator>>> {
        self.pipe.clone()
    }

    pub fn execute(&self, conn: &dyn ThreadedDataRowProvider) -> Result<()> {
        // initiate querying data
        conn.query_data_threaded(
            self.table_name.as_str(),
            self.column_defs.clone(),
            self.pipe.clone(),
        )?;

        Ok(())
    }
}

impl TableDefinition {
    ///
    /// Get header definition
    pub fn header(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }
    ///
    /// Loads table and returns `TableData`
    pub fn load(self, conn: &dyn DataRowProvider) -> Result<TableData> {
        let mut table_data = TableData {
            table_name: self.table_name,
            column_defs: Rc::new(self.columns),
            data: Vec::new(),
        };

        let data = conn.query_data(
            table_data.table_name.as_str(),
            table_data.column_defs.clone(),
        )?;
        table_data.data = data;

        Ok(table_data)
    }

    pub fn load_threaded(self) -> Result<ThreadedTableData> {
        // Create threaded data structure
        let threaded_data = ThreadedTableData {
            table_name: self.table_name,
            column_defs: Rc::new(self.columns),
            pipe: Arc::new(RwLock::new(VecDeque::new())),
        };
        // return pipe
        Ok(threaded_data)
    }
}

impl TableData {
    ///
    /// Returns rows
    pub fn rows(&self) -> &[DataRow] {
        self.data.as_slice()
    }

    ///
    /// Gets iterator over column definitions
    pub fn column_defs(
        &self,
    ) -> std::collections::btree_map::Values<'_, std::string::String, ColumnDefinition> {
        self.column_defs.values()
    }

    ///
    /// Get header definition
    pub fn header(&self) -> Vec<String> {
        self.column_defs.keys().cloned().collect()
    }
}

///
/// Implementing `Serialize` allows `ColumnValue` to be used directly with
/// serde's subclasses, like writing data directly into a csv file.
impl Serialize for ColumnValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ColumnValue::Boolean(v) => serializer.serialize_bool(*v),
            ColumnValue::Date(v) => {
                serializer.serialize_str(v.format("%Y-%m-%d").to_string().as_str())
            }
            ColumnValue::DateTime(v) => {
                serializer.serialize_str(v.format("%Y-%m-%d %H:%M:%S").to_string().as_str())
            }
            ColumnValue::Number(v) => serializer.serialize_i64(*v),
            ColumnValue::Float(v) => serializer.serialize_f64(*v),
            ColumnValue::Varchar(v) => serializer.serialize_str(v.as_str()),
        }
    }
}

impl Serialize for DataRow {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_seq(Some(self.column_values.len()))?;

        for column_value in self.column_values.iter() {
            map.serialize_element(&column_value)?;
        }

        map.end()
    }
}

impl Serialize for TableData {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.data.len() + 1))?;

        // add header
        let column_names: Vec<&str> = self
            .column_defs
            .values()
            .map(|df| df.column_name.as_str())
            .collect();
        seq.serialize_element(&column_names)?;

        for row_item in self.rows() {
            seq.serialize_element(&row_item)?;
        }

        seq.end()
    }
}

impl DataRow {
    ///
    /// Get column definitions for row
    pub fn column_defs(&self) -> Rc<BTreeMap<String, ColumnDefinition>> {
        self.column_defs.clone()
    }
}

impl ColumnDefinition {
    ///
    /// Gets nullable status for column
    pub fn nullable(&self) -> bool {
        self.nullable
    }
}
