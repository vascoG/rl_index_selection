from index_selection_evaluation.selection.utils import b_to_mb
import logging


class RewardCalculator(object):
    def __init__(self):
        self.reset()

    def reset(self):
        self.accumulated_reward = 0

    def calculate_reward(self, environment_state):
        current_cost = environment_state["current_cost"]
        previous_cost = environment_state["previous_cost"]
        initial_cost = environment_state["initial_cost"]

        reward = self._calculate_reward(current_cost, previous_cost, initial_cost)

        self.accumulated_reward += reward

        return reward

    def _calculate_reward(self, current_cost, previous_cost, initial_cost):
        raise NotImplementedError


# class AbsoluteDifferenceRelativeToStorageReward(RewardCalculator):
#     def __init__(self):
#         RewardCalculator.__init__(self)

#     def _calculate_reward(self, current_cost, previous_cost, initial_cost, new_partition_size):
#         reward = (previous_cost - current_cost) / new_partition_size

#         return reward


class AbsoluteDifferenceToPreviousReward(RewardCalculator):
    def __init__(self):
        RewardCalculator.__init__(self)

    def _calculate_reward(self, current_cost, previous_cost, initial_cost):
        reward = previous_cost - current_cost

        return reward


class RelativeDifferenceToPreviousReward(RewardCalculator):
    def __init__(self):
        RewardCalculator.__init__(self)

    def _calculate_reward(self, current_cost, previous_cost, initial_cost):
        reward = (previous_cost - current_cost) / initial_cost
        if reward == 0:
            return -1

        return reward


# class RelativeDifferenceRelativeToStorageReward(RewardCalculator):
#     def __init__(self):
#         RewardCalculator.__init__(self)

#         self.SCALER = 1

#     def _calculate_reward(self, current_cost, previous_cost, initial_cost, new_partition_size):
#         logging.info("current_cost: %s, previous_cost: %s, initial_cost: %s, new_partition_size: %s")
#         assert new_partition_size >= 0

#         reward = ((previous_cost - current_cost) / initial_cost) / b_to_mb(new_partition_size) * self.SCALER

#         return reward
