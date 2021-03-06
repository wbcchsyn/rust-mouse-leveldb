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

use core::ptr::NonNull;
use leveldb_sys::*;
use std::os::raw::c_uchar;

/// `Options` is a wrapper of `*mut leveldb_options_t` to make sure to destruct on the drop.
pub struct Options(NonNull<leveldb_options_t>);

unsafe impl Send for Options {}
unsafe impl Sync for Options {}

impl Drop for Options {
    fn drop(&mut self) {
        unsafe { leveldb_options_destroy(self.0.as_ptr()) };
    }
}

impl Options {
    /// Creates a new instance.
    pub fn new() -> Self {
        unsafe {
            let ptr = leveldb_options_create();
            assert_eq!(false, ptr.is_null());

            const TRUE: c_uchar = 1;
            const FALSE: c_uchar = 0;
            leveldb_options_set_create_if_missing(ptr, TRUE);
            leveldb_options_set_error_if_exists(ptr, FALSE);
            leveldb_options_set_paranoid_checks(ptr, TRUE);

            Self(NonNull::new_unchecked(ptr))
        }
    }

    /// Provides a raw pointer to wrapped address.
    pub fn as_ptr(&self) -> *const leveldb_options_t {
        self.0.as_ptr()
    }
}
