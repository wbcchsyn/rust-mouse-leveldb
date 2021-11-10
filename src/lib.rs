// Copyright 2021 Shin Yoshida
//
// "LGPL-3.0-or-later OR Apache-2.0 OR BSD-2-Clause"
//
// This is part of mouse-leveldb
//
//  mouse-leveldb is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  mouse-leveldb is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with mouse-leveldb.  If not, see <http://www.gnu.org/licenses/>.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
// Redistribution and use in source and binary forms, with or without modification, are permitted
// provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of
//    conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice, this
//    list of conditions and the following disclaimer in the documentation and/or other
//    materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

//! `mouse-leveldb` is a wrapper of crate `leveldb-sys` for `mouse` .

#![deny(missing_docs)]

mod database;
mod error;
mod octets;
mod options;
mod read_options;
mod write_batch;
mod write_options;

use core::ptr::{null_mut, NonNull};
use core::result::Result;
pub use database::Database;
pub use error::Error;
use leveldb_sys::*;
pub use octets::Octets;
use once_cell::sync::Lazy;
use options::Options;
use read_options::ReadOptions;
use std::os::raw::c_char;
pub use write_batch::WriteBatch;
use write_options::WriteOptions;

static OPTIONS: Lazy<Options> = Lazy::new(|| Options::new());
static READ_OPTIONS: Lazy<ReadOptions> = Lazy::new(|| ReadOptions::new());
static WRITE_OPTIONS: Lazy<WriteOptions> = Lazy::new(|| WriteOptions::new());

/// Flushes `batch` to `db` .
/// After this method is called, `batch` will be cleared even if failed.
///
/// # Panics
///
/// Causes a panic if `db` is not opened.
///
/// # Examples
///
/// ```
/// use mouse_leveldb::{Database, WriteBatch};
/// use std::ffi::CString;
/// use tempfile;
///
/// let tmp = tempfile::tempdir().unwrap();
/// let path = CString::new(tmp.path().to_str().unwrap()).unwrap();
///
/// let mut db = Database::new();
/// db.open(&path).unwrap();
///
/// let mut batch = WriteBatch::new();
///
/// let key1: &[u8] = &[1, 2, 3];
/// let key2: &[u8] = &[4];
///
/// let value1: &[u8] = &[];
/// let value2: &[u8] = &[5, 6];
/// let value3: &[u8] = &[7, 7, 8];
///
/// batch.put(key1, value1);
/// batch.put(key2, value2);
/// batch.put(key1, value3);
///
/// mouse_leveldb::write(&db, &mut batch);
/// ```
pub fn write(db: &Database, batch: &mut WriteBatch) -> Result<(), Error> {
    match write_batch::as_ptr(batch) {
        None => Ok(()),
        Some(batch) => {
            let mut error: *mut c_char = null_mut();
            let errptr: *mut *mut c_char = &mut error;

            unsafe {
                leveldb_write(
                    database::as_ptr(db).unwrap(),
                    WRITE_OPTIONS.as_ptr(),
                    batch,
                    errptr,
                );
                leveldb_writebatch_clear(batch);
            }

            match NonNull::new(error) {
                None => Ok(()),
                Some(ptr) => unsafe { Err(error::new(ptr)) },
            }
        }
    }
}

/// Tries to fetch the value corresponding to `key` .
///
/// If no such `key` is stored, returns an empty [`Octets`] .
/// (It is not an error because the query itself is succeeded.)
///
/// # Panics
///
/// Causes a panic if `db` is not opened.
///
/// # Examples
///
/// ```
/// use mouse_leveldb::{Database, WriteBatch};
/// use std::ffi::CString;
/// use tempfile;
///
/// let tmp = tempfile::tempdir().unwrap();
/// let path = CString::new(tmp.path().to_str().unwrap()).unwrap();
///
/// let mut db = Database::new();
/// db.open(&path).unwrap();
///
/// let key: &[u8] = &[1, 2, 3];
/// let value: &[u8] = &[4, 4];
///
/// // Not found before insert.
/// {
///     let octets = mouse_leveldb::get(&db, key);
///     assert_eq!(&[] as &[u8], octets.unwrap().as_ref());
/// }
///
/// let mut batch = WriteBatch::new();
///
/// batch.put(key, value);
/// mouse_leveldb::write(&db, &mut batch);
///
/// // Found the value after insert.
/// {
///     let octets = mouse_leveldb::get(&db, key);
///     assert_eq!(value, octets.unwrap().as_ref());
/// }
/// ```
#[inline]
pub fn get(db: &Database, key: &[u8]) -> Result<Octets, Error> {
    let mut error: *mut c_char = null_mut();
    let errptr: *mut *mut c_char = &mut error;

    let mut vallen: usize = 0;

    unsafe {
        let pval = leveldb_get(
            database::as_ptr(db).unwrap(),
            READ_OPTIONS.as_ptr(),
            key.as_ptr() as *const c_char,
            key.len(),
            &mut vallen as *mut usize,
            errptr,
        );

        match NonNull::new(error) {
            Some(ptr) => Err(error::new(ptr)),
            None => Ok(octets::new(pval as *mut u8, vallen)),
        }
    }
}
