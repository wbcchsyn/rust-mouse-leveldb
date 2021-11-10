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

use leveldb_sys::{
    leveldb_writebatch_clear, leveldb_writebatch_create, leveldb_writebatch_destroy,
    leveldb_writebatch_put, leveldb_writebatch_t,
};
use std::os::raw::c_char;

/// `WriteBatch` is a wrapper of `*mut leveldb_writebatch_t` to make sure to destruct on the drop.
pub struct WriteBatch {
    ptr: Option<*mut leveldb_writebatch_t>,
    len_: usize,
}

unsafe impl Send for WriteBatch {}
unsafe impl Sync for WriteBatch {}

impl Drop for WriteBatch {
    fn drop(&mut self) {
        self.destroy();
    }
}

impl WriteBatch {
    /// Creates a new instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use mouse_leveldb::WriteBatch;
    ///
    /// let _batch = WriteBatch::new();
    /// ```
    pub const fn new() -> Self {
        Self { ptr: None, len_: 0 }
    }

    /// Appends a pair of `(key, value)` to self.
    ///
    /// # Warnings
    ///
    /// This method calls `leveldb_sys::leveldb_writebatch_put` and it copies `key` and `value`
    /// internally.
    ///
    /// Accumerating too many raws may exhaust the OS memory.
    ///
    /// # Examples
    ///
    /// ```
    /// use mouse_leveldb::WriteBatch;
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
    /// ```
    #[inline]
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        unsafe {
            let ptr = match self.ptr {
                None => {
                    let ptr = leveldb_writebatch_create();
                    self.ptr = Some(ptr);
                    ptr
                }
                Some(ptr) => ptr,
            };

            leveldb_writebatch_put(
                ptr,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len(),
            );
        }
    }

    /// Deletes the holding keys and values.
    ///
    /// # Examples
    ///
    /// ```
    /// use mouse_leveldb::WriteBatch;
    ///
    /// let mut batch = WriteBatch::new();
    ///
    /// batch.clear();  // Do nothing.
    ///
    /// let key: &[u8] = &[1, 2, 3];
    /// let value: &[u8] = &[];
    /// batch.put(key, value);
    ///
    /// batch.clear();  // Delete 'key' and 'value'
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        if let Some(ptr) = self.ptr {
            unsafe { leveldb_writebatch_clear(ptr) };
        }
    }

    /// Makes sure to destructs the wrapped pointer.
    ///
    /// # Examples
    ///
    /// ```
    /// use mouse_leveldb::WriteBatch;
    ///
    /// let mut batch = WriteBatch::new();
    /// batch.destroy();
    /// ```
    pub fn destroy(&mut self) {
        if let Some(ptr) = self.ptr {
            unsafe { leveldb_writebatch_destroy(ptr) };
            self.ptr = None;
        }
    }
}

/// Returns a pointer to the wrapped address.
pub fn as_ptr(batch: &mut WriteBatch) -> Option<*mut leveldb_writebatch_t> {
    batch.ptr
}
