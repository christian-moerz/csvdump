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
//! Builder for table selection
//!

use super::meta::ColumnDataProvider;
use super::{ColumnDefinition, TableDefinition};
use crate::Error;
use crate::Result;
use std::collections::{BTreeMap, BTreeSet};

///
/// Builds `TableDefinition` from a few simple inputs.
///
pub struct TableSelectionBuilder {
    /// table name
    table_name: String,
    /// selection of columns to query
    column_names: BTreeSet<String>,
}

impl TableSelectionBuilder {
    ///
    /// Constructs a new `TableSelectionBuilder`
    pub fn new<S: AsRef<str>>(table_name: S) -> TableSelectionBuilder {
        TableSelectionBuilder {
            table_name: String::from(table_name.as_ref()),
            column_names: BTreeSet::new(),
        }
    }

    /// Adds a column name
    pub fn with<S: AsRef<str>>(mut self, column_name: S) -> Self {
        self.column_names.insert(String::from(column_name.as_ref()));

        self
    }

    ///
    /// Constructs a `TableDefinition` from given column and table data
    pub fn build(self, conn: &dyn ColumnDataProvider) -> Result<TableDefinition> {
        info!("Querying table column data.");
        // get the columns
        let columns = conn.query_column_data(&self.table_name)?;

        info!("Checking whether we have unknown columns.");

        if columns.is_empty() {
            warn!("Column query returned no data.");
        } else {
            debug!("Query returned {} columns.", columns.len());
        }

        // check whether there are columns being queried that are not in that table?
        let known_columns: BTreeSet<&str> =
            columns.iter().map(|col| col.column_name.as_str()).collect();
        let queried_names: BTreeSet<&str> =
            self.column_names.iter().map(|col| col.as_str()).collect();
        let unknown_columns: BTreeSet<&str> =
            queried_names.difference(&known_columns).cloned().collect();

        if !unknown_columns.is_empty() {
            // take the first unknown column and complain
            return Err(Error::UnknownColumn(
                unknown_columns.iter().next().unwrap().to_string(),
            ));
        }

        info!("Filtering to queried columns.");

        // filter to the columns we want
        let filtered: BTreeMap<String, ColumnDefinition> = columns
            .into_iter()
            .filter(|col| self.column_names.contains(&col.column_name))
            .map(|col| (col.column_name.clone(), col))
            .collect();

        info!("Returning table definition.");

        Ok(TableDefinition {
            table_name: self.table_name,
            columns: filtered,
        })
    }
}
