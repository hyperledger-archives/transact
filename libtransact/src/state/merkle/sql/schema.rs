/*
 * Copyright 2021 Cargill Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

// Due to schema differences, this may have to be under a submodule specific to sqlite or postgres
// (or other array-supporting dbs). This may only be the case with the merkle_radix_leaf table.
table! {
    merkle_radix_tree (id) {
        id -> Int8,
        name -> VarChar,
    }
}

table! {
    merkle_radix_leaf (id) {
        id -> Int8,
        tree_id -> Int8,
        address -> VarChar,
        data -> Blob,
    }
}

table! {
    merkle_radix_tree_node (hash, tree_id) {
        hash -> VarChar,
        tree_id -> Int8,
        leaf_id -> Nullable<Int8>,
        // JSON children
        children -> Text,
    }
}

table! {
    merkle_radix_state_root (id) {
        id -> Int8,
        tree_id -> Int8,
        state_root -> VarChar,
        parent_state_root -> VarChar,
    }
}

table! {
    merkle_radix_state_root_leaf_index (id) {
        id -> Int8,
        tree_id -> Int8,
        leaf_id -> Int8,
        from_state_root_id -> Int8,
        to_state_root_id -> Nullable<Int8>,
    }
}

joinable!(merkle_radix_state_root_leaf_index -> merkle_radix_leaf (leaf_id));

allow_tables_to_appear_in_same_query!(
    merkle_radix_tree,
    merkle_radix_leaf,
    merkle_radix_tree_node,
    merkle_radix_state_root,
    merkle_radix_state_root_leaf_index,
);
