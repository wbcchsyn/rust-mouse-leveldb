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

use crate::error::{self, Error};
use crate::OPTIONS;
use core::ptr::{null_mut, NonNull};
use leveldb_sys::{leveldb_close, leveldb_open, leveldb_t};
use std::ffi::CStr;
use std::os::raw::c_char;

/// `Database` is a wrapper of `*mut leveldb_t` to make sure to close on the drop.
pub struct Database(Option<*mut leveldb_t>);

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Drop for Database {
    fn drop(&mut self) {
        self.close();
    }
}

impl Database {
    /// Creates a new instance with unopened state.
    ///
    /// # Examples
    ///
    /// ```
    /// use mouse_leveldb::Database;
    ///
    /// let _db = Database::new();
    /// ```
    pub const fn new() -> Self {
        Self(None)
    }

    /// Creates a database if not exists and opens.
    ///
    /// `path` is the path to the directory where database files are stored.
    ///
    /// # Panics
    ///
    /// Causes a panic if `self` has been already opened.
    ///
    /// # Examples
    ///
    /// ```
    /// use mouse_leveldb::Database;
    /// use std::ffi::CString;
    /// use tempfile;
    ///
    /// let tmp = tempfile::tempdir().unwrap();
    /// let path = CString::new(tmp.path().to_str().unwrap()).unwrap();
    ///
    /// let mut db = Database::new();
    /// db.open(&path).unwrap();
    /// ```
    pub fn open(&mut self, path: &CStr) -> Result<(), Error> {
        assert_eq!(None, self.0);

        unsafe {
            let mut error: *mut c_char = null_mut();
            let errptr: *mut *mut c_char = &mut error;

            let ptr = leveldb_open(OPTIONS.as_ptr(), path.as_ptr(), errptr);
            match NonNull::new(error) {
                Some(e) => {
                    assert_eq!(true, ptr.is_null());
                    Err(error::new(e))
                }
                None => {
                    assert_eq!(false, ptr.is_null());
                    self.0 = Some(ptr);
                    Ok(())
                }
            }
        }
    }

    /// Closes the DB and makes `self` unopend state if opened; otherwise does nothing.
    pub fn close(&mut self) {
        if let Some(ptr) = self.0 {
            unsafe { leveldb_close(ptr) };
            self.0 = None;
        }
    }
}

/// Returns a pointer to the wrapped address.
///
/// Note that `leveldb_t` is `Sync` .
pub fn as_ptr(db: &Database) -> Option<*mut leveldb_t> {
    db.0
}
