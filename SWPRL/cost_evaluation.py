import datetime
import logging

from .what_if_partition_creation import WhatIfPartitionCreation


class CostEvaluation:
    def __init__(self, db_connector, cost_estimation="whatif"):
        logging.debug("Init cost evaluation")
        self.db_connector = db_connector
        self.cost_estimation = cost_estimation
        logging.info("Cost estimation with " + self.cost_estimation)
        self.what_if = WhatIfPartitionCreation(db_connector)
        self.current_partitions = set()

        if len(self.current_partitions) != 0:
            assert len(self.what_if.all_simulated_partitions()) == len(self.current_partitions)

        self.cost_requests = 0
        self.cache_hits = 0
        # Cache structure:
        # {(query_object, relevant_partitions): cost}
        self.cache = {}

        # Cache structure:
        # {(query_object, relevant_partitions): (cost, plan)}
        self.cache_plans = {}

        self.completed = False
        # It is not necessary to drop hypothetical partitions during __init__().
        # These are only created per connection. Hence, non should be present.

        self.relevant_partitions_cache = {}

        self.costing_time = datetime.timedelta(0)

    def estimate_size(self, partition):
        # TODO: Refactor: It is currently too complicated to compute
        # We must search in current partitions to get an partition object with .hypopg_oid
        result = None
        for i in self.current_partitions:
            if partition == i:
                result = i
                break
        if result:
            # Partition does currently exist and size can be queried
            if not partition.estimated_size:
                partition.estimated_size = self.what_if.estimate_partition_size(result.table_name)
        else:
            raise NotImplementedError("Partition does not exist and size cannot be queried.")
            #self._simulate_or_create_index(partition, store_size=True) # TODO

    # def which_indexes_utilized_and_cost(self, query, indexes):
    #     self._prepare_cost_calculation(indexes, store_size=True)

    #     plan = self.db_connector.get_plan(query)
    #     cost = plan["Total Cost"]
    #     plan_str = str(plan)

    #     recommended_indexes = set()

    #     # We are iterating over the CostEvalution's indexes and not over `indexes`
    #     # because it is not guaranteed that hypopg_name is set for all items in
    #     # `indexes`. This is caused by _prepare_cost_calculation that only creates
    #     # indexes which are not yet existing. If there is no hypothetical index
    #     # created for an index object, there is no hypopg_name assigned to it. However,
    #     # all items in current_partitions must also have an equivalent in `indexes`.
    #     for index in self.current_partitions:
    #         assert (
    #             index in indexes
    #         ), "Something went wrong with _prepare_cost_calculation."

    #         if index.hypopg_name not in plan_str:
    #             continue
    #         recommended_indexes.add(index)

    #     return recommended_indexes, cost

    # def calculate_cost(self, workload, indexes, store_size=False):
    #     assert (
    #         self.completed is False
    #     ), "Cost Evaluation is completed and cannot be reused."
    #     self._prepare_cost_calculation(indexes, store_size=store_size)
    #     total_cost = 0

    #     # TODO: Make query cost higher for queries which are running often
    #     for query in workload.queries:
    #         self.cost_requests += 1
    #         total_cost += self._request_cache(query, indexes) * query.frequency
    #     return total_cost

    def calculate_cost_and_plans(self, workload, partitions, store_size=False):
        assert (
            self.completed is False
        ), "Cost Evaluation is completed and cannot be reused."
        start_time = datetime.datetime.now()

        self._prepare_cost_calculation(partitions, store_size=store_size)
        total_cost = 0
        plans = []
        costs = []

        for query in workload.queries:
            self.cost_requests += 1
            cost, plan = self._request_cache_plans(query, partitions)
            total_cost += cost * query.frequency
            plans.append(plan)
            costs.append(cost)

        end_time = datetime.datetime.now()
        self.costing_time += end_time - start_time

        return total_cost, plans, costs

    # Creates the current partition combination by simulating/creating
    # missing partitions and unsimulating/dropping partitions
    # that exist but are not in the combination.
    def _prepare_cost_calculation(self, partitions, store_size=False):
        for partition in set(partitions) - self.current_partitions:
            self._simulate_or_create_partition(partition, store_size=store_size)
        for partition in self.current_partitions - set(partitions):
            self._unsimulate_or_drop_partition(partition)

        assert self.current_partitions == set(partitions)

    def _simulate_or_create_partition(self, partition, store_size=False):
        if self.cost_estimation == "whatif":
            self.what_if.simulate_partition(partition, store_size=store_size)
        else:
            raise NotImplementedError("Actual runtimes are not supported yet.")
        self.current_partitions.add(partition)

    def _unsimulate_or_drop_partition(self, partition):
        if self.cost_estimation == "whatif":
            self.what_if.drop_simulated_partition(partition)
        else:
            raise NotImplementedError("Actual runtimes are not supported yet.")
        self.current_partitions.remove(partition)

    def _get_cost(self, query):
        if self.cost_estimation == "whatif":
            return self.db_connector.get_cost(query)
        else:
            raise NotImplementedError("Actual runtimes are not supported yet.")

    def _get_cost_plan(self, query):
        query_plan = self.db_connector.get_plan(query)
        return query_plan["Total Cost"], query_plan

    def complete_cost_estimation(self):
        self.completed = True

        for partition in self.current_partitions.copy():
            self._unsimulate_or_drop_partition(partition)

        assert self.current_partitions == set()

    def _request_cache(self, query, partitions):
        q_i_hash = (query, frozenset(partitions))
        if q_i_hash in self.relevant_partitions_cache:
            relevant_partitions = self.relevant_partitions_cache[q_i_hash]
        else:
            relevant_partitions = self._relevant_partitions(query, partitions)
            self.relevant_partitions_cache[q_i_hash] = relevant_partitions

        # Check if query and corresponding relevant partitions in cache
        if (query, relevant_partitions) in self.cache:
            self.cache_hits += 1
            return self.cache[(query, relevant_partitions)]
        # If no cache hit request cost from database system
        else:
            cost = self._get_cost(query)
            self.cache[(query, relevant_partitions)] = cost
            return cost

    def _request_cache_plans(self, query, partitions):
        q_i_hash = (query, frozenset(partitions))
        if q_i_hash in self.relevant_partitions_cache:
            relevant_partitions = self.relevant_partitions_cache[q_i_hash]
        else:
            relevant_partitions = self._relevant_partitions(query, partitions)
            self.relevant_partitions_cache[q_i_hash] = relevant_partitions

        # Check if query and corresponding relevant partitions in cache
        if (query, relevant_partitions) in self.cache:
            self.cache_hits += 1
            return self.cache[(query, relevant_partitions)]
        # If no cache hit request cost from database system
        else:
            cost, plan = self._get_cost_plan(query)
            self.cache[(query, relevant_partitions)] = (cost, plan)
            return cost, plan

    @staticmethod
    def _relevant_partitions(query, partitions):
        relevant_partitions = [
            x.column for x in partitions
        ]
        return frozenset(relevant_partitions)
