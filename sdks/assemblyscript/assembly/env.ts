// Copyright 2018-2020 Cargill Incorporated
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

export declare function get_state(addresses: i32): i32;
export declare function set_state(addr_data: i32): i32;
export declare function delete_state(addresses: i32): i32;
export declare function get_ptr_len(ptr: i32): isize;
export declare function alloc(len: usize): i32;
export declare function read_byte(offset: isize): u8;
export declare function write_byte(ptr: i32, offset:u32, byte: u8): i32;
export declare function get_ptr_from_collection(ptr: i32, index: i32): i32;
export declare function get_ptr_collection_len(ptr: i32): isize;
export declare function add_to_collection(head: i32, ptr: i32): i32; 
export declare function create_collection(head: i32): i32;
export declare function log_buffer(log_level: i32, log_string: i32): void;
export declare function log_level(): i32;
