-- Copyright 2022 Cargill Incorporated
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

PRAGMA foreign_keys=OFF;

CREATE TABLE new_merkle_radix_leaf (
    id INTEGER PRIMARY KEY,
    tree_id INTEGER NOT NULL,
    address STRING NOT NULL,
    data BLOB,
    FOREIGN KEY(tree_id) REFERENCES merkle_radix_tree (id)
);

INSERT INTO new_merkle_radix_leaf
    (id, tree_id, address, data)
    SELECT id, tree_id, address, data
    FROM merkle_radix_leaf;

DROP TABLE merkle_radix_leaf;

ALTER TABLE new_merkle_radix_leaf RENAME TO merkle_radix_leaf;

PRAGMA foreign_keys=ON;
