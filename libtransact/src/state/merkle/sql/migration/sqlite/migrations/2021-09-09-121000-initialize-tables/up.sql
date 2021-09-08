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

CREATE TABLE IF NOT EXISTS merkle_radix_tree (
    id INTEGER PRIMARY KEY,
    name TEXT,
    UNIQUE(name)
);

INSERT INTO merkle_radix_tree (name) VALUES ('default');

CREATE TABLE IF NOT EXISTS merkle_radix_leaf (
    id INTEGER PRIMARY KEY,
    tree_id INTEGER NOT NULL,
    address STRING NOT NULL,
    data BLOB,
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree (id)
);

CREATE TABLE IF NOT EXISTS merkle_radix_tree_node (
    hash STRING NOT NULL,
    tree_id INTEGER NOT NULL,
    leaf_id INTEGER,
    children TEXT,
    PRIMARY KEY (hash, tree_id),
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree (id),
    FOREIGN KEY(leaf_id) REFERENCES merkle_radix_leaf(id)
);

create TABLE IF NOT EXISTS merkle_radix_change_log_addition (
    id INTEGER PRIMARY KEY,
    tree_id BIGINT NOT NULL,
    state_root VARCHAR(64) NOT NULL,
    parent_state_root VARCHAR(64),
    addition VARCHAR(64),
    FOREIGN KEY(state_root, tree_id) REFERENCES merkle_radix_tree_node(hash, tree_id),
    FOREIGN KEY(parent_state_root, tree_id) REFERENCES merkle_radix_tree_node(hash, tree_id),
    FOREIGN KEY(addition, tree_id) REFERENCES merkle_radix_tree_node(hash, tree_id)
);

create TABLE IF NOT EXISTS merkle_radix_change_log_deletion (
    id INTEGER PRIMARY KEY,
    tree_id BIGINT NOT NULL,
    successor_state_root VARCHAR(64) NOT NULL,
    state_root VARCHAR(64) NOT NULL,
    deletion VARCHAR(64),
    FOREIGN KEY(successor_state_root, tree_id) REFERENCES merkle_radix_tree_node(hash, tree_id),
    FOREIGN KEY(state_root, tree_id) REFERENCES merkle_radix_tree_node(hash, tree_id),
    FOREIGN KEY(deletion, tree_id) REFERENCES merkle_radix_tree_node(hash, tree_id)
);
