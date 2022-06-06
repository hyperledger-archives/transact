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

PRAGMA foreign_keys=OFF;

ALTER TABLE merkle_radix_tree_node
    ADD COLUMN reference INTEGER NOT NULL DEFAULT 0;

-- Count the references via the additions. In Sqlite, this is accomplished by
-- copying the data to a temporary table, and reinserting it using a join with
-- the counts from the additions.
CREATE TABLE IF NOT EXISTS temp_merkle_radix_tree_node (
    hash STRING NOT NULL,
    tree_id INTEGER NOT NULL,
    leaf_id INTEGER,
    children TEXT,
    refernce INTEGER,
    PRIMARY KEY (hash, tree_id)
);

INSERT INTO temp_merkle_radix_tree_node select * from merkle_radix_tree_node;

DELETE from merkle_radix_tree_node;

INSERT into merkle_radix_tree_node
SELECT n.hash, n.tree_id, n.leaf_id, n.children, ref_counts.reference
FROM temp_merkle_radix_tree_node n,
    (SELECT addition, tree_id, COUNT(*) AS reference
          FROM merkle_radix_change_log_addition
          GROUP BY addition, tree_id) AS ref_counts
WHERE n.hash = ref_counts.addition
  AND n.tree_id = ref_counts.tree_id;

DROP TABLE temp_merkle_radix_tree_node;

ALTER TABLE merkle_radix_leaf ADD COLUMN pruned_at INTEGER;

ALTER TABLE merkle_radix_change_log_addition ADD COLUMN pruned_at INTEGER;

CREATE INDEX IF NOT EXISTS
    idx_merkle_change_log_add_tree_id_state_root
  ON merkle_radix_change_log_addition(tree_id, state_root);

CREATE INDEX IF NOT EXISTS
    idx_merkle_change_log_add_tree_id_parent_state_root
  ON merkle_radix_change_log_addition(tree_id, parent_state_root);

ALTER TABLE merkle_radix_change_log_deletion ADD COLUMN pruned_at INTEGER;

CREATE INDEX IF NOT EXISTS
    idx_merkle_change_log_delete_tree_id_state_root
  ON merkle_radix_change_log_deletion(tree_id, state_root);

CREATE INDEX IF NOT EXISTS
    idx_merkle_change_log_delete_tree_id_successor_state_root
  ON merkle_radix_change_log_deletion(tree_id, successor_state_root);

PRAGMA foreign_keys=ON;
