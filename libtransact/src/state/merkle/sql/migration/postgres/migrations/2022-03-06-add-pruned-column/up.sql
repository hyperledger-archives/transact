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

ALTER TABLE merkle_radix_tree_node
    ADD COLUMN IF NOT EXISTS reference BIGINT NOT NULL DEFAULT 0;

-- Count the references via the additions
UPDATE merkle_radix_tree_node
SET reference = ref_counts.reference
FROM (SELECT addition, tree_id, COUNT(*) AS reference
      FROM merkle_radix_change_log_addition
      GROUP BY addition, tree_id) AS ref_counts
WHERE merkle_radix_tree_node.hash = ref_counts.addition
  AND merkle_radix_tree_node.tree_id = ref_counts.tree_id;

ALTER TABLE merkle_radix_leaf
    ADD COLUMN IF NOT EXISTS pruned_at BIGINT;

ALTER TABLE merkle_radix_change_log_addition
    ADD COLUMN IF NOT EXISTS pruned_at BIGINT;

CREATE INDEX IF NOT EXISTS
    idx_merkle_change_log_add_tree_id_state_root
  ON merkle_radix_change_log_addition(tree_id, state_root);

CREATE INDEX IF NOT EXISTS
    idx_merkle_change_log_add_tree_id_parent_state_root
  ON merkle_radix_change_log_addition(tree_id, parent_state_root);

ALTER TABLE merkle_radix_change_log_deletion
    ADD COLUMN IF NOT EXISTS pruned_at BIGINT;

CREATE INDEX IF NOT EXISTS
    idx_merkle_change_log_delete_tree_id_state_root
  ON merkle_radix_change_log_deletion(tree_id, state_root);

CREATE INDEX IF NOT EXISTS
    idx_merkle_change_log_delete_tree_id_successor_state_root
  ON merkle_radix_change_log_deletion(tree_id, successor_state_root);
