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
//! Configuration for accessing database
//!

use oracle::Connection;
use std::fs::read_to_string;
use std::path::Path;
use toml::from_str;

///
/// Database configuration
#[derive(Deserialize)]
pub struct Config {
    dbhost: String,
    dbname: String,
    dbuser: String,
    dbpass: String,
}

impl Config {
    ///
    /// Connects to database via specified credentials
    pub fn connect(self) -> Result<Connection, oracle::Error> {
        Connection::connect(
            &self.dbuser,
            &self.dbpass,
            format!("//{}/{}", self.dbhost, self.dbname),
        )
    }

    pub fn load(filename: &Path) -> Result<Config, Box<dyn std::error::Error>> {
        if !filename.exists() {
            eprintln!("File {} not found.", filename.to_string_lossy());
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "File not found",
            )));
        }

        let contents = read_to_string(filename)?;

        Ok(from_str(&contents)?)
    }
}
