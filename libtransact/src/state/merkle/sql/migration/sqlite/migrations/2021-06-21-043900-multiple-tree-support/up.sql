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

INSERT INTO merkle_radix_tree (id, name) VALUES (1, 'default');

-- Rename all the current tables to _old:
ALTER TABLE merkle_radix_state_root_leaf_index
    RENAME TO _merkle_radix_state_root_leaf_index_old;

ALTER TABLE merkle_radix_state_root RENAME TO _merkle_radix_state_root_old;

ALTER TABLE merkle_radix_tree_node RENAME TO _merkle_radix_tree_node_old;

ALTER TABLE merkle_radix_leaf RENAME TO _merkle_radix_leaf_old;

-- Recreate the tables with a tree ID, in the order of foreign key relationships

-- Add tree_id to the merkle_radix_leaf table
CREATE TABLE IF NOT EXISTS merkle_radix_leaf (
    id INTEGER PRIMARY KEY,
    tree_id INTEGER NOT NULL,
    address STRING NOT NULL,
    data BLOB,
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree (id)
);

INSERT INTO merkle_radix_leaf
    (id, tree_id, address, data)
    SELECT id, 1, address, data FROM _merkle_radix_leaf_old;

-- Add tree_id to the merkle_radix_tree_node table
CREATE TABLE IF NOT EXISTS merkle_radix_tree_node (
    hash STRING NOT NULL,
    tree_id INTEGER NOT NULL,
    leaf_id INTEGER,
    children TEXT,
    PRIMARY KEY (hash, tree_id),
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree(id),
    FOREIGN KEY(leaf_id) REFERENCES merkle_radix_leaf(id)
);

INSERT INTO merkle_radix_tree_node
    (hash, tree_id, leaf_id, children)
    SELECT hash, 1, leaf_id, children FROM _merkle_radix_tree_node_old;

-- Add tree_id to the merkle_radix_state_root table
CREATE TABLE IF NOT EXISTS merkle_radix_state_root (
    id INTEGER PRIMARY KEY,
    tree_id INTEGER NOT NULL,
    state_root STRING NOT NULL,
    parent_state_root STRING NOT NULL,
    FOREIGN KEY(state_root, tree_id) REFERENCES merkle_radix_tree_node(hash, tree_id)
);

INSERT INTO merkle_radix_state_root
    (id, tree_id, state_root, parent_state_root)
    SELECT id, 1, state_root, parent_state_root
    FROM _merkle_radix_state_root_old;

-- Add tree_id to the merkle_radix_state_root_leaf_index table
CREATE TABLE IF NOT EXISTS merkle_radix_state_root_leaf_index (
    id INTEGER PRIMARY KEY,
    leaf_id INTEGER NOT NULL,
    tree_id INTEGER NOT NULL,
    from_state_root_id INTEGER NOT NULL,
    to_state_root_id INTEGER,
    FOREIGN KEY(from_state_root_id) REFERENCES merkle_radix_state_root(id),
    FOREIGN KEY(leaf_id) REFERENCES merkle_radix_leaf (id),
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree (id)
);

INSERT INTO merkle_radix_state_root_leaf_index
    (id, leaf_id, tree_id, from_state_root_id, to_state_root_id)
    SELECT id, leaf_id, 1, from_state_root_id, to_state_root_id
    FROM _merkle_radix_state_root_leaf_index_old;

-- Drop the old tables
DROP TABLE _merkle_radix_state_root_leaf_index_old;
DROP TABLE _merkle_radix_state_root_old;
DROP TABLE _merkle_radix_tree_node_old;
DROP TABLE _merkle_radix_leaf_old;
