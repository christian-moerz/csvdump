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
//! Oracle implementation for meta
//!

use super::meta::{ColumnDataProvider, DataRowProvider, ThreadedDataRowProvider};
use super::{ColumnDefinition, ColumnValue, DataRow, DataType, RowIndicator};
use crate::Error;
use crate::Result;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::sync::{Arc, RwLock};

impl ColumnDataProvider for oracle::Connection {
    fn query_column_data(&self, table_name: &str) -> Result<Vec<ColumnDefinition>> {
        let mut owner: Option<String> = None;

        // check whether owner is specified in front of table name
        let t_name: String = if let Some(cut_index) = table_name.find('.') {
            debug!("Owner included in table name. Separating.");

            let mut dupl: String = String::from(table_name);

            let new_name: String = dupl.split_off(cut_index + 1);
            // split out point
            let _ = dupl.split_off(cut_index);

            debug!("Identified owner [{}]", &dupl);
            owner = Some(dupl);

            debug!("Identified table name [{}]", &new_name);

            new_name
        } else {
            String::from(table_name)
        };
        // construct query statement for getting column data
        let query: &str = match &owner {
            None => {
                r#"SELECT COLUMN_NAME, NULLABLE, DATA_TYPE, DATA_LENGTH, DATA_PRECISION FROM ALL_TAB_COLUMNS WHERE TABLE_NAME=:1"#
            }
            Some(_) => {
                r#"SELECT COLUMN_NAME, NULLABLE, DATA_TYPE, DATA_LENGTH, DATA_PRECISION FROM ALL_TAB_COLUMNS WHERE TABLE_NAME=:1 AND OWNER=:2"#
            }
        };

        debug!("Attempting query: {}", query);
        debug!("Param :1 is {}", t_name);
        if let Some(o) = &owner {
            debug!("Param :2 is {}", o);
        }

        // query data from database
        let rows = match &owner {
            None => self.query(query, &[&t_name])?,
            Some(o) => self.query(query, &[&t_name.to_string(), &o.to_string()])?,
        };

        debug!("Got rows in return.");

        let mut result_vec: Vec<ColumnDefinition> = Vec::new();

        debug!("Iterating {} rows...", result_vec.len());

        for row_result in rows {
            debug!("Attempting to resolve result set.");
            let row = row_result?;

            debug!("Getting column name.");
            let column_name: String = row.get("COLUMN_NAME")?;
            let nullable_str: String = row.get("NULLABLE")?;
            debug!("Getting data type.");
            let data_type: String = row.get("DATA_TYPE")?;
            debug!("Getting data length.");
            let data_length: Option<u32> = row.get("DATA_LENGTH")?;
            debug!("Getting data precision.");
            let data_precision: Option<u32> = row.get("DATA_PRECISION")?;
            debug!("Getting nullable.");
            let nullable: bool = "Y" == nullable_str;

            debug!("Converting to internal data type.");
            let data_type = match data_type.as_str() {
                "NUMBER" => DataType::Number(data_length.unwrap_or(0), data_precision.unwrap_or(0)),
                "VARCHAR2" => DataType::VarChar(data_length.unwrap_or(0)),
                "DATE" => DataType::Date,
                "TIMESTAMP(6)" => DataType::DateTime,
                "BOOL" => DataType::Boolean,
                "CLOB" => DataType::CLob,
                x => return Err(Error::UnknownDataType(String::from(x))),
            };

            debug!("Pushing result structure into return vector.");
            result_vec.push(ColumnDefinition {
                column_name,
                nullable,
                data_type,
            });
        }

        debug!("Row iteration completed.");
        Ok(result_vec)
    }
}

impl DataRowProvider for oracle::Connection {
    ///
    /// queries data from database
    fn query_data<'row>(
        &self,
        table_name: &str,
        column_names: Rc<BTreeMap<String, ColumnDefinition>>,
    ) -> Result<Vec<DataRow>> {
        // collect column names into comma separated string
        let column_str: String = column_names
            .values()
            .map(|s| s.column_name.as_str())
            .collect::<Vec<&str>>()
            .join(",");
        // build query
        let query: String = format!(r#"SELECT {} FROM {}"#, column_str, table_name);

        // query data from database
        let rows = self.query(&query, &[])?;

        let mut result_vec: Vec<DataRow> = Vec::new();

        for row_result in rows {
            let row = row_result?;
            let values_result: Result<Vec<Option<ColumnValue>>> = column_names
                .values()
                .map(|col_item| {
                    Ok(match col_item.data_type {
                        DataType::VarChar(_) | DataType::CLob => {
                            let data: Option<String> = row.get(col_item.column_name.as_str())?;

                            match data {
                                Some(v) => Some(ColumnValue::Varchar(v)),
                                None => None,
                            }
                        }
                        DataType::Number(_, precision) => {
                            if precision > 0 {
                                let data: Option<f64> = row.get(col_item.column_name.as_str())?;
                                match data {
                                    Some(v) => Some(ColumnValue::Float(v)),
                                    None => None,
                                }
                            } else {
                                let data: Option<i64> = row.get(col_item.column_name.as_str())?;
                                match data {
                                    Some(v) => Some(ColumnValue::Number(v)),
                                    None => None,
                                }
                            }
                        }
                        DataType::Boolean => {
                            let data: Option<bool> = row.get(col_item.column_name.as_str())?;

                            data.map(ColumnValue::Boolean)
                        }
                        DataType::Date => {
                            let data: Option<DateTime<Utc>> =
                                row.get(col_item.column_name.as_str())?;

                            data.map(ColumnValue::Date)
                        }
                        DataType::DateTime => {
                            let data: Option<DateTime<Utc>> =
                                row.get(col_item.column_name.as_str())?;

                            data.map(ColumnValue::DateTime)
                        }
                    })
                })
                .collect();
            let column_values: Vec<Option<ColumnValue>> = values_result?;

            result_vec.push(DataRow {
                column_defs: column_names.clone(),
                column_values,
            });
        }

        Ok(result_vec)
    }
}

impl ThreadedDataRowProvider for oracle::Connection {
    fn query_data_threaded(
        &self,
        table_name: &str,
        column_names: Rc<BTreeMap<String, ColumnDefinition>>,
        q: Arc<RwLock<VecDeque<RowIndicator>>>,
    ) -> Result<()> {
        // collect column names into comma separated string
        let column_str: String = column_names
            .values()
            .map(|s| s.column_name.as_str())
            .collect::<Vec<&str>>()
            .join(",");
        // build query
        let query: String = format!(r#"SELECT {} FROM {}"#, column_str, table_name);

        // query data from database
        let rows = self.query(&query, &[])?;

        for row_result in rows {
            let row = row_result?;
            let values_result: Result<Vec<Option<ColumnValue>>> = column_names
                .values()
                .map(|col_item| {
                    Ok(match col_item.data_type {
                        DataType::VarChar(_) | DataType::CLob => {
                            let data: Option<String> = row.get(col_item.column_name.as_str())?;

                            match data {
                                Some(v) => Some(ColumnValue::Varchar(v)),
                                None => None,
                            }
                        }
                        DataType::Number(_, precision) => {
                            if precision > 0 {
                                let data: Option<f64> = row.get(col_item.column_name.as_str())?;
                                match data {
                                    Some(v) => Some(ColumnValue::Float(v)),
                                    None => None,
                                }
                            } else {
                                let data: Option<i64> = row.get(col_item.column_name.as_str())?;
                                match data {
                                    Some(v) => Some(ColumnValue::Number(v)),
                                    None => None,
                                }
                            }
                        }
                        DataType::Boolean => {
                            let data: Option<bool> = row.get(col_item.column_name.as_str())?;

                            data.map(ColumnValue::Boolean)
                        }
                        DataType::Date => {
                            let data: Option<DateTime<Utc>> =
                                row.get(col_item.column_name.as_str())?;

                            data.map(ColumnValue::Date)
                        }
                        DataType::DateTime => {
                            let data: Option<DateTime<Utc>> =
                                row.get(col_item.column_name.as_str())?;

                            data.map(ColumnValue::DateTime)
                        }
                    })
                })
                .collect();
            let column_values: Vec<Option<ColumnValue>> = values_result?;

            match q.write() {
                Ok(mut queue_in) => {
                    queue_in.push_back(RowIndicator::MoreToCome(column_values));
                }
                Err(e) => {
                    error!(
                        "Failed to push data entry because queue could not be unlocked: {}",
                        e
                    );
                }
            };
        }

        match q.write() {
            Ok(mut queue_in) => queue_in.push_back(RowIndicator::EndOfData),
            Err(e) => {
                error!(
                    "Failed to push finalization indicator. This will lead to deadlock: {}",
                    e
                );
                panic!("Avoiding deadlock.");
            }
        };

        Ok(())
    }
}
