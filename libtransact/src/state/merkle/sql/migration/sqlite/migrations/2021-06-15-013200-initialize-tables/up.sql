-- Copyright 2021 Cargill Incorporated
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS merkle_radix_leaf (
    id INTEGER PRIMARY KEY,
    address STRING NOT NULL,
    data BLOB
);

CREATE TABLE IF NOT EXISTS merkle_radix_tree_node (
    hash STRING PRIMARY KEY,
    leaf_id INTEGER,
    children TEXT,
    FOREIGN KEY(leaf_id) REFERENCES merkle_radix_leaf(id)
);

CREATE TABLE IF NOT EXISTS merkle_radix_state_root (
    id INTEGER PRIMARY KEY,
    state_root STRING NOT NULL,
    parent_state_root STRING NOT NULL,
    FOREIGN KEY(state_root) REFERENCES merkle_radix_tree_node (hash)
);

CREATE TABLE IF NOT EXISTS merkle_radix_state_root_leaf_index (
    id INTEGER PRIMARY KEY,
    leaf_id INTEGER NOT NULL,
    from_state_root_id INTEGER NOT NULL,
    to_state_root_id INTEGER,
    FOREIGN KEY(from_state_root_id) REFERENCES merkle_radix_state_root(id),
    FOREIGN KEY(leaf_id) REFERENCES merkle_radix_leaf (id)
);
