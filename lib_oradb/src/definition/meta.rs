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
//! Meta definitions for querying meta data
//!

use super::{ColumnDefinition, DataRow, RowIndicator};
use crate::Result;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::sync::{Arc, RwLock};

///
/// Provides column data from a database
pub trait ColumnDataProvider {
    ///
    /// queries column data
    fn query_column_data(&self, table_name: &str) -> Result<Vec<ColumnDefinition>>;
}

pub trait DataRowProvider {
    ///
    /// queries data rows
    fn query_data(
        &self,
        table_name: &str,
        column_names: Rc<BTreeMap<String, ColumnDefinition>>,
    ) -> Result<Vec<DataRow>>;
}

///
/// A provider that pushes read data into a data queue instead
/// of returning all items collectively.
pub trait ThreadedDataRowProvider {
    ///
    /// queries data rows in threaded fashion
    fn query_data_threaded(
        &self,
        table_name: &str,
        column_names: Rc<BTreeMap<String, ColumnDefinition>>,
        q: Arc<RwLock<VecDeque<RowIndicator>>>,
    ) -> Result<()>;
}
