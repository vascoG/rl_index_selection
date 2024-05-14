import collections
import copy
import logging
import random

import gym

from gym_db.common import EnvironmentType
from SWPRL.cost_evaluation import CostEvaluation
from index_selection_evaluation.selection.dbms.postgres_dbms import PostgresDatabaseConnector
from SWPRL.partition import Partition
from index_selection_evaluation.selection.utils import b_to_mb


class DBEnvV2(gym.Env):
    def __init__(self, environment_type=EnvironmentType.TRAINING, config=None):
        super(DBEnvV2, self).__init__()

        self.rnd = random.Random()
        self.rnd.seed(config["random_seed"])
        self.env_id = config["env_id"]
        self.environment_type = environment_type
        self.config = config

        self.number_of_resets = 0
        self.total_number_of_steps = 0

        self.connector = PostgresDatabaseConnector(config["database_name"], autocommit=True)
        self.connector.drop_indexes()
        self.cost_evaluation = CostEvaluation(self.connector)

        self.globally_partitionable_columns_flat = config["globally_partitionable_columns_flat"]

        self.all_partitions = config["all_partitions"]
        self.all_partitions_flat = config["all_partitions_flat"]

        # In certain cases, workloads are consumed: therefore, we need copy
        self.workloads = copy.copy(config["workloads"])
        self.current_workload_idx = 0
        self.similar_workloads = config["similar_workloads"]
        self.max_steps_per_episode = config["max_steps_per_episode"]

        self.action_manager = config["action_manager"]
        self.action_manager.test_variable = self.env_id
        self.action_space = self.action_manager.get_action_space()

        self.observation_manager = config["observation_manager"]
        self.observation_space = self.observation_manager.get_observation_space()

        self.reward_calculator = config["reward_calculator"]

        self._init_modifiable_state()

        if self.environment_type != environment_type.TRAINING:
            self.episode_performances = collections.deque(maxlen=len(config["workloads"]))

    def reset(self):
        self.number_of_resets += 1
        self.total_number_of_steps += self.steps_taken

        initial_observation = self._init_modifiable_state()

        return initial_observation

    def _step_asserts(self, action):
        assert self.action_space.contains(action), f"{action} ({type(action)}) invalid"
        assert (
            self.valid_actions[action] == self.action_manager.ALLOWED_ACTION
        ), f"Agent has chosen invalid action: {action} - {self.all_partitions_flat[action]}"
        assert (
            self.all_partitions_flat[action] not in self.current_partitions
        ), f"{self.all_partitions_flat[action]} already in self.current_partitions"

    def step(self, action):
        self._step_asserts(action)

        self.steps_taken += 1

        new_partition = self.all_partitions_flat[action]
        self.current_partitions.add(new_partition)

        environment_state = self._update_return_env_state(
            init=False, new_partition=new_partition
        )
        current_observation = self.observation_manager.get_observation(environment_state)

        self.valid_actions, is_valid_action_left = self.action_manager.update_valid_actions(
            action
        )
        episode_done = self.steps_taken >= self.max_steps_per_episode or not is_valid_action_left

        reward = self.reward_calculator.calculate_reward(environment_state)

        if episode_done and self.environment_type != EnvironmentType.TRAINING:
            self._report_episode_performance(environment_state)
            self.current_workload_idx += 1
            print(f"Partitions: {self.current_partitions}")

        return current_observation, reward, episode_done, {"action_mask": self.valid_actions}

    def _report_episode_performance(self, environment_state):
        episode_performance = {
            "achieved_cost": self.current_costs / self.initial_costs * 100,
            "evaluated_workload": self.current_workload,
            "partitions": self.current_partitions,
        }

        output = (
            f"Evaluated Workload ({self.environment_type}): {self.current_workload}\n    "
            f"Initial cost: {self.initial_costs:,.2f}, now: {self.current_costs:,.2f} "
            f"({episode_performance['achieved_cost']:.2f}). Reward: {self.reward_calculator.accumulated_reward}.\n    "
        )
        logging.warning(output)

        self.episode_performances.append(episode_performance)

    def _init_modifiable_state(self):
        self.current_partitions = set()
        self.steps_taken = 0
        self.reward_calculator.reset()

        if len(self.workloads) == 0:
            self.workloads = copy.copy(self.config["workloads"])

        if self.environment_type == EnvironmentType.TRAINING:
            if self.similar_workloads:
                # 200 is an arbitrary value
                self.current_workload = self.workloads.pop(0 + self.env_id * 200)
            else:
                self.current_workload = self.rnd.choice(self.workloads)
        else:
            self.current_workload = self.workloads[self.current_workload_idx % len(self.workloads)]

        self.previous_cost = None

        self.valid_actions = self.action_manager.get_initial_valid_actions(self.current_workload)
        environment_state = self._update_return_env_state(init=True)

        state_fix_for_episode = {
            "workload": self.current_workload,
            "initial_cost": self.initial_costs,
        }
        self.observation_manager.init_episode(state_fix_for_episode)

        initial_observation = self.observation_manager.get_observation(environment_state)

        return initial_observation

    def _update_return_env_state(self, init, new_partition=None):
        total_costs, plans_per_query, costs_per_query = self.cost_evaluation.calculate_cost_and_plans(
            self.current_workload, self.current_partitions
        )

        if not init:
            self.previous_cost = self.current_costs

        self.current_costs = total_costs

        if init:
            self.initial_costs = total_costs

        environment_state = {
            "action_status": self.action_manager.current_action_status,
            "current_cost": self.current_costs,
            "previous_cost": self.previous_cost,
            "initial_cost": self.initial_costs,
            "plans_per_query": plans_per_query,
            "costs_per_query": costs_per_query,
        }

        return environment_state

    def get_cost_eval_cache_info(self):
        return self.cost_evaluation.cost_requests, self.cost_evaluation.cache_hits, self.cost_evaluation.costing_time

    def get_cost_eval_cache(self):
        return self.cost_evaluation.cache

    # BEGIN OF NOT IMPLEMENTED ##########
    def render(self, mode="human"):
        print("render() was called")
        pass

    def close(self):
        print("close() was called")

    # END OF NOT IMPLEMENTED ##########
