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

#[cfg(feature = "sqlite")]
table! {
    #[sql_name = "merkle_radix_tree_node"]
    sqlite_merkle_radix_tree_node (hash, tree_id) {
        hash -> VarChar,
        tree_id -> Int8,
        leaf_id -> Nullable<Int8>,
        // JSON children
        children -> Text,
    }
}

#[cfg(feature = "postgres")]
table! {
    #[sql_name = "merkle_radix_tree_node"]
    postgres_merkle_radix_tree_node (hash, tree_id) {
        hash -> VarChar,
        tree_id -> Int8,
        leaf_id -> Nullable<Int8>,
        children -> Array<Nullable<VarChar>>,
    }
}

table! {
    merkle_radix_change_log_addition (id) {
        id -> Int8,
        tree_id -> Int8,
        state_root -> VarChar,
        parent_state_root -> Nullable<VarChar>,
        addition -> VarChar,
    }
}

table! {
    merkle_radix_change_log_deletion (id) {
        id -> Int8,
        tree_id -> Int8,
        successor_state_root -> VarChar,
        state_root -> VarChar,
        deletion -> VarChar,
    }
}

#[cfg(all(feature = "sqlite", feature = "postgres"))]
allow_tables_to_appear_in_same_query!(
    merkle_radix_tree,
    merkle_radix_leaf,
    sqlite_merkle_radix_tree_node,
    postgres_merkle_radix_tree_node,
);

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
allow_tables_to_appear_in_same_query!(
    merkle_radix_tree,
    merkle_radix_leaf,
    sqlite_merkle_radix_tree_node,
);

#[cfg(all(not(feature = "sqlite"), feature = "postgres"))]
allow_tables_to_appear_in_same_query!(
    merkle_radix_tree,
    merkle_radix_leaf,
    postgres_merkle_radix_tree_node,
);
