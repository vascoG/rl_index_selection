import logging

import numpy as np
from gym import spaces


class ObservationManager(object):
    def __init__(self, number_of_actions):
        self.number_of_actions = number_of_actions

    def _init_episode(self, state_fix_for_episode):

        self.initial_cost = state_fix_for_episode["initial_cost"]

    def init_episode(self, state_fix_for_episode):
        raise NotImplementedError

    def get_observation(self, environment_state):
        raise NotImplementedError

    def get_observation_space(self):
        observation_space = spaces.Box(
            low=self._create_low_boundaries(), high=self._create_high_boundaries(), shape=self._create_shape()
        )

        logging.info(f"Creating ObservationSpace with {self.number_of_features} features.")

        return observation_space

    def _create_shape(self):
        return (self.number_of_features,)

    def _create_low_boundaries(self):
        low = [-np.inf for feature in range(self.number_of_features)]

        return np.array(low)

    def _create_high_boundaries(self):
        high = [np.inf for feature in range(self.number_of_features)]

        return np.array(high)


class EmbeddingObservationManager(ObservationManager):
    def __init__(self, number_of_actions, config):
        ObservationManager.__init__(self, number_of_actions)

        self.workload_embedder = config["workload_embedder"]
        self.representation_size = self.workload_embedder.representation_size
        self.workload_size = config["workload_size"]

        self.number_of_features = (
            self.number_of_actions  # Indicates for each action whether it was taken or not
            + (
                self.representation_size * self.workload_size
            )  # embedding representation for every query in the workload
            + self.workload_size  # The frequencies for every query in the workloads
            + 1  # The initial workload cost
            + 1  # The current workload cost
        )

    def _init_episode(self, state_fix_for_episode):
        episode_workload = state_fix_for_episode["workload"]
        self.frequencies = np.array(EmbeddingObservationManager._get_frequencies_from_workload(episode_workload))

        super()._init_episode(state_fix_for_episode)

    def init_episode(self, state_fix_for_episode):
        raise NotImplementedError

    def get_observation(self, environment_state):
        if self.UPDATE_EMBEDDING_PER_OBSERVATION:
            workload_embedding = np.array(self.workload_embedder.get_embeddings(environment_state["plans_per_query"]))
        else:
            # In this case the workload embedding is not updated with every step but also not set during init
            if self.workload_embedding is None:
                self.workload_embedding = np.array(
                    self.workload_embedder.get_embeddings(environment_state["plans_per_query"])
                )

            workload_embedding = self.workload_embedding

        observation = np.array(environment_state["action_status"])
        observation = np.append(observation, workload_embedding)
        observation = np.append(observation, self.frequencies)
        observation = np.append(observation, self.initial_cost)
        observation = np.append(observation, environment_state["current_cost"])

        return observation

    @staticmethod
    def _get_frequencies_from_workload(workload):
        frequencies = []
        for query in workload.queries:
            frequencies.append(query.frequency)
        return frequencies

class PartitionPlanEmbeddingObservationManagerWithCost(EmbeddingObservationManager):
    def __init__(self, number_of_actions, config):
        super().__init__(number_of_actions, config)

        self.UPDATE_EMBEDDING_PER_OBSERVATION = True

        # This overwrites EmbeddingObservationManager's features
        self.number_of_features = (
            self.number_of_actions  # Indicates for each action whether it was taken or not
            + (
                self.representation_size * self.workload_size
            )  # embedding representation for every query in the workload
            + self.workload_size  # The costs for every query in the workload
            + self.workload_size  # The frequencies for every query in the workloads
            + 1  # The initial workload cost
            + 1  # The current workload cost
        )

    def init_episode(self, state_fix_for_episode):
        super()._init_episode(state_fix_for_episode)

    # This overwrite EmbeddingObservationManager.get_observation() because further features are added
    def get_observation(self, environment_state):
        workload_embedding = np.array(self.workload_embedder.get_embeddings(environment_state["plans_per_query"]))
        observation = np.array(environment_state["action_status"])
        observation = np.append(observation, workload_embedding)
        observation = np.append(observation, environment_state["costs_per_query"])
        observation = np.append(observation, self.frequencies)
        observation = np.append(observation, self.initial_cost)
        observation = np.append(observation, environment_state["current_cost"])

        return observation

class PartitionPlanEmbeddingObservationManager(EmbeddingObservationManager):
    def __init__(self, number_of_actions, config):
        super().__init__(number_of_actions, config)

        self.UPDATE_EMBEDDING_PER_OBSERVATION = True

        # This overwrites EmbeddingObservationManager's features
        self.number_of_features = (
            self.number_of_actions  # Indicates for each action whether it was taken or not
            + (
                self.representation_size * self.workload_size
            )  # embedding representation for every query in the workload
            + 1  # The initial workload cost
            + 1  # The current workload cost
        )

    def init_episode(self, state_fix_for_episode):
        super()._init_episode(state_fix_for_episode)

    # This overwrite EmbeddingObservationManager.get_observation() because further features are added
    def get_observation(self, environment_state):
        workload_embedding = np.array(self.workload_embedder.get_embeddings(environment_state["plans_per_query"]))
        observation = np.array(environment_state["action_status"])
        observation = np.append(observation, workload_embedding)
        observation = np.append(observation, self.initial_cost)
        observation = np.append(observation, environment_state["current_cost"])

        return observation

