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

extern crate clap;
extern crate toml;
#[macro_use]
extern crate serde;
extern crate colored;
extern crate csv;
extern crate lib_oradb;
extern crate log;
extern crate oracle;
extern crate simplelog;

mod config;

use clap::{App, Arg};
use colored::*;
use config::Config;
use lib_oradb::definition::TableSelectionBuilder;
use lib_oradb::definition::RowIndicator;
use std::path::Path;
use std::sync::{Arc,RwLock};

const VERSION: &str = env!("CARGO_PKG_VERSION");

///
/// Reads column names from file
fn read_parameters_file(
    filename: &Path,
    uppercase_flag: bool,
) -> Result<Vec<String>, std::io::Error> {
    let fulltext = std::fs::read_to_string(filename)?;
    let separated_lines: Vec<&str> = fulltext.lines().collect();
    let cleaned_cols: Vec<String> = separated_lines
        .into_iter()
        .map(|colname| {
            if uppercase_flag {
                String::from(colname.trim()).to_uppercase()
            } else {
                String::from(colname.trim())
            }
        })
        .filter(|colname| colname.len() > 0)
        .collect();

    Ok(cleaned_cols)
}

fn main() {
    let matches = App::new("CSV TABLE DUMP")
        .version(VERSION)
        .author("Christian Moerz <chris@ny-central.org>")
        .about("Exports Oracle database table data into CSV")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .value_name("FILE")
                .help("Sets output filename")
                .takes_value(true)
                .default_value("output.csv"),
        )
        .arg(
            Arg::with_name("quoteall")
                .short("q")
                .long("quoteall")
                .help("Puts quotation marks around all values"),
        )
        .arg(
            Arg::with_name("force")
                .short("f")
                .long("force")
                .help("Overwrites existing output file if set"),
        )
        .arg(
            Arg::with_name("uppercase")
                .short("u")
                .long("uppercase")
                .help("Uppercase all column names"),
        )
        .arg(
            Arg::with_name("tablename")
                .short("n")
                .long("tablename")
                .help("Overrides table name (default is input filename)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file to use")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    if matches.occurrences_of("v") > 0 {
        let _ = simplelog::SimpleLogger::init(
            match matches.occurrences_of("v") {
                1 => log::LevelFilter::Error,
                2 => log::LevelFilter::Warn,
                3 => log::LevelFilter::Info,
                4 => log::LevelFilter::Debug,
                _ => log::LevelFilter::Trace,
            },
            simplelog::Config::default(),
        );
    }

    let start_stamp = std::time::SystemTime::now();

    let config_name = matches.value_of("config").unwrap_or("config.toml");
    println!("Using configuration file {}.", config_name.yellow());
    let config = match Config::load(&std::path::PathBuf::from(config_name)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "Configuration file {} {} to load: {}",
                config_name.yellow(),
                "failed".red(),
                e
            );
            std::process::exit(5);
        }
    };

    // we can unwrap INPUT because it's a required parameter
    let data_file = matches.value_of("INPUT").unwrap();

    let force_flag = matches.is_present("force");
    let quote_flag = matches.is_present("quoteall");
    let uppercase_flag = matches.is_present("uppercase");
    let output_file = matches.value_of("output").unwrap();

    let output_file_path = std::path::PathBuf::from(output_file);
    if output_file_path.exists() & !force_flag {
        eprintln!(
            "Output file {} exists but force flag not set. {}",
            output_file.yellow(),
            "Will not overwrite.".red()
        );
        std::process::exit(14);
    }

    let data_file_path = std::path::PathBuf::from(data_file);
    if !data_file_path.exists() {
        eprintln!("Input file {} {}.", data_file.yellow(), "not found".red());
        std::process::exit(5);
    }
    println!("Loading input file {}.", data_file.yellow());
    let column_names = match read_parameters_file(&data_file_path, uppercase_flag) {
        Ok(cn) => cn,
        Err(e) => {
            eprintln!(
                "Reading input file {} {}: {}",
                data_file.yellow(),
                "failed".red(),
                e
            );
            std::process::exit(2)
        }
    };

    println!(
        "Input file requests {} columns:",
        column_names.len().to_string().blue()
    );
    for cn in &column_names {
        println!("{} * {}", " ".repeat(10), cn.blue());
    }
    println!("Attempting database connection.");
    let conn = match config.connect() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Database connection {}: {}", "failed".red(), e);
            std::process::exit(10);
        }
    };
    println!("Database connection {}.", "succeeded".green());

    // if table name is overridden by input parameter, take user specified
    // table name, otherwise attempt to extract from input filename
    let table_name: String = match matches.value_of("tablename") {
        Some(tn) => String::from(tn),
        None => match data_file_path.file_stem() {
            Some(st) => st.to_string_lossy().to_string(),
            None => {
                eprintln!(
                    "{} to extract table name from file name {}.",
                    "Failed".red(),
                    data_file.yellow()
                );
                std::process::exit(11);
            }
        },
    };

    println!(
        "Attempting to read table definition for {}.",
        table_name.blue()
    );

    // set up table selection builder to construct
    // meta data query about table column information
    let mut builder = TableSelectionBuilder::new(&table_name);
    for cn in &column_names {
        // add specified column names
        builder = builder.with(cn);
    }

    // run "build" to get table definition
    let table_def = match builder.build(&conn) {
        Ok(df) => df,
        Err(e) => {
            eprintln!(
                "{} to read table definition for table {}: {}",
                "Failed".red(),
                table_name.yellow(),
                e
            );
            std::process::exit(12);
        }
    };
    println!(
        "{} read table definition for table {}.",
        "Successfully".green(),
        table_name.blue()
    );

    // create output writer
    let csv_build = if quote_flag {
        csv::WriterBuilder::new().quote_style(csv::QuoteStyle::Always).from_path(output_file_path)
    } else {
        csv::Writer::from_path(output_file_path)
    };
    let mut csv_out = match csv_build {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "{} to create CSV output file {}: {}",
                "Failed".red(),
                output_file.yellow(),
                e
            );
            std::process::exit(15);
        }
    };

    // write csv header
    csv_out
        .serialize(table_def.header())
        .expect("Failed to serialize header.");

    // laod the data
    let data = match table_def.load_threaded() {
        Ok(dt) => dt,
        Err(e) => {
            eprintln!(
                "{} to read data for table {}: {}",
                "Failed".red(),
                table_name.yellow(),
                e
            );
            std::process::exit(13);
        }
    };

    let counter: Arc<RwLock<u64>> = Arc::new(RwLock::new(0));
    let thread_count = counter.clone();
    let thread_queue = data.pipe().clone();
    let t_handle = std::thread::spawn(move || {
        let mut error_count: u16 = 0;
        loop {
            let is_empty: bool = match thread_queue.read() {
                Ok(q) => q.is_empty(),
                Err(e) => {
                    eprintln!(
                        "{} to acquire read lock on data queue: {}",
                        "Failed".red(),
                        e
                    );
                    error_count += 1;

                    if error_count > 3 {
                        panic!("Failed to acquire read lock beyond threshold.");
                    }

                    true
                }
            };
            if is_empty {
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            }

            let next_row : RowIndicator = match thread_queue.write() {
                Ok(mut q) => {
                    match q.pop_front() {
                        Some(i) => i,
                        None => {
                            eprintln!("Failed to retrieve element from queue.");
                            continue;
                        }
                    }
                },
                Err(e) => {
                    eprintln!(
                        "{} to acquire read lock on data queue: {}",
                        "Failed".red(),
                        e
                    );
                    error_count += 1;

                    if error_count > 3 {
                        panic!("Failed to acquire read lock beyond threshold.");
                    } else {
                        continue;
                    }
                }
            };

            match next_row {
                RowIndicator::MoreToCome(row) => csv_out.serialize(row).expect("Failed to serialize row."),
                RowIndicator::EndOfData => break
            };

            match thread_count.write() {
                Ok(mut c) => *c += 1,
                Err(e) => eprintln!("{} to increment row counter: {}", "Failed".red(), e )
            };
        }
    });

    match data.execute(&conn) {
        Ok(()) => println!("Database loading completed {}.", "successfully".green()),
        Err(e) => eprintln!("{} during database loading: {}", "Failure".red(), e )
    };

    println!("Waiting for writer thread to complete.");
    if let Err(e) = t_handle.join() {
        eprintln!("{} waiting for writer thread: {:?}", "Failed".red(), e );
    } else {
        println!("Writer thread shut down {}", "successfully".green());
    }

    /*for row in data.rows() {
        csv_out.serialize(row).expect("Failed to serialize row.");
        counter += 1;
    }*/

    match counter.read() {
        Ok(c) => println!(
            "{} completed writing {} rows.",
            "Successfully".green(),
            (*c).to_string().green()
        ),
        Err(e) => eprintln!("{} to calculate final row count: {}", "Failed".red(), e ),
    };

    match start_stamp.elapsed() {
        Ok(t) => println!("Task completed in {} seconds.", t.as_secs()),
        Err(e) => eprintln!("{} to measure elapsed time: {}", "Failed".red(), e)
    };
}
