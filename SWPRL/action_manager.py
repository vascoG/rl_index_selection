import copy
import logging

import numpy as np
from gym import spaces

from index_selection_evaluation.selection.utils import b_to_mb

FORBIDDEN_ACTION_SB3 = -np.inf
ALLOWED_ACTION_SB3 = 0

FORBIDDEN_ACTION_SB2 = 0
ALLOWED_ACTION_SB2 = 1


class ActionManager(object):
    def __init__(self, sb_version):
        self.valid_actions = None
        self._remaining_valid_actions = None
        self.number_of_actions = None
        self.current_action_status = None

        self.test_variable = None

        self.sb_version = sb_version

        if self.sb_version == 2:
            self.FORBIDDEN_ACTION = FORBIDDEN_ACTION_SB2
            self.ALLOWED_ACTION = ALLOWED_ACTION_SB2
        else:
            self.FORBIDDEN_ACTION = FORBIDDEN_ACTION_SB3
            self.ALLOWED_ACTION = ALLOWED_ACTION_SB3

    def get_action_space(self):
        return spaces.Discrete(self.number_of_actions)

    def get_initial_valid_actions(self, workload):
        self.current_action_status = [0 for action in range(self.number_of_actions)]

        self.valid_actions = [self.FORBIDDEN_ACTION for action in range(self.number_of_actions)]
        self._remaining_valid_actions = []

        self._valid_actions_based_on_workload(workload)

        self.current_partitions = set()

        return np.array(self.valid_actions)

    def update_valid_actions(self, last_action):
        # if last_action == len(self.valid_actions) - 1:
        #     for action_idx in copy.copy(self._remaining_valid_actions):
        #         self.valid_actions[action_idx] = self.FORBIDDEN_ACTION
        #         self._remaining_valid_actions.remove(action_idx)
        #     return np.array(self.valid_actions), False
        assert self.all_partitions_flat[last_action] not in self.current_partitions


        self.current_action_status[last_action] += 1
    
        self.current_partitions.add(self.all_partitions_flat[last_action])

        self.valid_actions[last_action] = self.FORBIDDEN_ACTION
        self._remaining_valid_actions.remove(last_action)

        self._valid_actions_based_on_last_action(last_action)

        is_valid_action_left = len(self._remaining_valid_actions) > 0

        return np.array(self.valid_actions), is_valid_action_left

    # def _valid_actions_based_on_budget(self, budget, current_storage_consumption):
    #     if budget is None:
    #         return
    #     else:
    #         new_remaining_actions = []
    #         for action_idx in self._remaining_valid_actions:
    #             if b_to_mb(current_storage_consumption + self.action_storage_consumptions[action_idx]) > budget:
    #                 self.valid_actions[action_idx] = self.FORBIDDEN_ACTION
    #             else:
    #                 new_remaining_actions.append(action_idx)

    #         self._remaining_valid_actions = new_remaining_actions

    def _valid_actions_based_on_workload(self, workload):
        raise NotImplementedError

    def _valid_actions_based_on_last_action(self, last_action):
        raise NotImplementedError


class PartitionActionManager(ActionManager):
    def __init__(
        self, partitionable_columns, sb_version, all_partitions
    ):
        ActionManager.__init__(self, sb_version)

        # partitionable_columns is a list of lists (representing partitionable columns on each table)
        self.partitionable_columns = partitionable_columns
        self.partitionable_columns_flat = [
            item for sublist in self.partitionable_columns for item in sublist
        ]

        self.all_partitions = all_partitions # all_partitions is a list of lists of lists (representing all partitions for each column for each table)

        self.all_partitions_flat = [
            subitem for sublist in self.all_partitions for item in sublist for subitem in item
        ]


        self.number_of_actions = len(self.all_partitions_flat)

    def _valid_actions_based_on_last_action(self, last_action):

        last_partition = self.all_partitions_flat[last_action]
        last_column = last_partition.column
        last_table = last_column.table

        for partition_idx in copy.copy(self._remaining_valid_actions):
            partition = self.all_partitions_flat[partition_idx]
            column = partition.column
            table = column.table
            if column.is_date() and column == last_column:
                self.valid_actions[partition_idx] = self.FORBIDDEN_ACTION
                self._remaining_valid_actions.remove(partition_idx)
            if table == last_table and column != last_column:
                self.valid_actions[partition_idx] = self.FORBIDDEN_ACTION
                self._remaining_valid_actions.remove(partition_idx)


    def _valid_actions_based_on_workload(self, workload):
        partitionable_columns = workload.partitionable_columns(return_sorted=False)
        partitionable_columns = partitionable_columns & frozenset(self.partitionable_columns_flat)
        self.wl_partitionable_columns = partitionable_columns

        for partition_idx, partition in enumerate(
                self.all_partitions_flat
        ):
            if partition.column in partitionable_columns:
                self.valid_actions[partition_idx] = self.ALLOWED_ACTION
                self._remaining_valid_actions.append(partition_idx)


        # assert np.count_nonzero(np.array(self.valid_actions) == self.ALLOWED_ACTION)-1 == 10*len(
        #     partitionable_columns
        # ), f"Mismatch partionable columns {len(partitionable_columns)} and valid actions {len(self.valid_actions)}."
