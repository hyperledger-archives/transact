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
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(512),
    UNIQUE(name)
);

INSERT INTO merkle_radix_tree (name) VALUES ('default');

CREATE TABLE IF NOT EXISTS merkle_radix_leaf (
    id BIGSERIAL PRIMARY KEY,
    tree_id BIGINT NOT NULL,
    address VARCHAR(70) NOT NULL,
    data BYTEA,
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree (id)
);

CREATE TABLE IF NOT EXISTS merkle_radix_tree_node (
    hash VARCHAR(64) NOT NULL,
    tree_id BIGINT NOT NULL,
    leaf_id BIGINT,
    children VARCHAR(64)[],
    PRIMARY KEY (hash, tree_id),
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree(id),
    FOREIGN KEY(leaf_id) REFERENCES merkle_radix_leaf(id)
);

CREATE TABLE IF NOT EXISTS merkle_radix_state_root (
    id BIGSERIAL PRIMARY KEY,
    tree_id BIGINT NOT NULL,
    state_root VARCHAR(64) NOT NULL,
    parent_state_root VARCHAR(64) NOT NULL,
    FOREIGN KEY(state_root, tree_id) REFERENCES merkle_radix_tree_node(hash, tree_id)
);

CREATE TABLE IF NOT EXISTS merkle_radix_state_root_leaf_index (
    id BIGSERIAL PRIMARY KEY,
    leaf_id BIGINT NOT NULL,
    tree_id BIGINT NOT NULL,
    from_state_root_id BIGINT NOT NULL,
    to_state_root_id BIGINT,
    FOREIGN KEY(from_state_root_id) REFERENCES merkle_radix_state_root(id),
    FOREIGN KEY(leaf_id) REFERENCES merkle_radix_leaf (id),
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree (id)
);
