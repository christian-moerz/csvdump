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
//! This library helps with reading data from Oracle databases
//! via simple column and table selection. Data types and sizes
//! are read directly from the database and thus do not need
//! to be provided separately.
//!

extern crate chrono;
extern crate oracle;
extern crate serde;
#[macro_use]
extern crate log;
extern crate csv;
extern crate simplelog;

pub mod definition;
mod error;

pub use self::error::Error;
/// Result redefinition for crate
pub type Result<E> = std::result::Result<E, Error>;

#[cfg(test)]
mod tests {
    use crate::definition::TableSelectionBuilder;
    use log::LevelFilter;
    use oracle::Connection;
    use simplelog::{Config, SimpleLogger};
    use std::fs::read_to_string;

    ///
    /// Test building query on auftrag table
    #[test]
    fn test_builder() {
        let _ = SimpleLogger::init(LevelFilter::Debug, Config::default());

        info!("Reading password.");

        let pwd: String = read_to_string("c:\\oracle\\pwd").expect("Failed to read key.");
        info!("Creating builder.");
        let builder = TableSelectionBuilder::new("AUFTRAG")
            .with("AU_AKTNR")
            .with("AU_NACHNAME")
            .with("AU_KAUFDAT")
            .with("AU_STORDAT")
            .with("AU_MAND");
        info!("Establishing db connection.");
        let conn = Connection::connect("moerz", pwd, "//pora1/elkab.world")
            .expect("Failed to connect to db.");

        info!("Creating table definition.");
        let table_def = builder.build(&conn).expect("Failed to build definition.");

        println!("Table definition: {:?}", table_def);

        let data = table_def.load(&conn).expect("Failed to load data.");
        println!("Resulting data: {:?}", data);
    }

    ///
    /// Test serialization
    #[test]
    fn test_csv_serialization() {
        let _ = SimpleLogger::init(LevelFilter::Debug, Config::default());

        info!("Reading password.");

        let pwd: String = read_to_string("c:\\oracle\\pwd").expect("Failed to read key.");
        info!("Creating builder.");
        let builder = TableSelectionBuilder::new("AUFTRAG")
            .with("AU_AKTNR")
            .with("AU_NACHNAME")
            .with("AU_KAUFDAT")
            .with("AU_STORDAT")
            .with("AU_MAND");
        info!("Establishing db connection.");
        let conn = Connection::connect("moerz", pwd, "//pora1/elkab.world")
            .expect("Failed to connect to db.");

        info!("Creating table definition.");
        let table_def = builder.build(&conn).expect("Failed to build definition.");

        println!("Table definition: {:?}", table_def);

        let data = table_def.load(&conn).expect("Failed to load data.");

        let mut csv_out =
            csv::Writer::from_path("test.csv").expect("Failed to set up csv output file.");

        csv_out
            .serialize(data.header())
            .expect("Failed to serialize header.");

        for row in data.rows() {
            csv_out.serialize(row).expect("Failed to serialize row.");
        }
    }
}
