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

ALTER TABLE merkle_radix_tree_node DROP COLUMN reference;

ALTER TABLE merkle_radix_leaf DROP COLUMN pruned_at;

ALTER TABLE merkle_radix_change_log_addition DROP COLUMN pruned_at;
DROP INDEX idx_merkle_change_log_add_tree_id_state_root;
DROP INDEX idx_merkle_change_log_add_tree_id_parent_state_root;

ALTER TABLE merkle_radix_change_log_deletion DROP COLUMN pruned_at;
DROP INDEX idx_merkle_change_log_delete_tree_id_state_root;
DROP INDEX idx_merkle_change_log_delete_tree_id_successor_state_root;
