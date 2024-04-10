import datetime
import logging
import decimal

from .what_if_partition_creation import WhatIfPartitionCreation
from .import utils

from .expression_parser import ExpressionParser


class CostEvaluation:
    def __init__(self, db_connector):
        logging.debug("Init cost evaluation")
        self.db_connector = db_connector
        self.what_if = WhatIfPartitionCreation(db_connector)
        self.current_partitions = set()

        if len(self.current_partitions) != 0:
            assert len(self.what_if.all_simulated_partitions()) == len(self.current_partitions)

        self.expression_parser = ExpressionParser()

        self.cost_requests = 0
        self.cache_hits = 0
        # Cache structure:
        # {(query_object, relevant_partitions): cost}
        self.cache = {}

        # Cache structure:
        # {(query_object, relevant_partitions): (cost, plan)}
        self.cache_plans = {}

        self.completed = False

        self.relevant_partitions_cache = {}

        self.cache_percentiles = {}

        self.cache_intervals = {}

        self.costing_time = datetime.timedelta(0)

    # def estimate_size(self, partition):
    #     # TODO: Refactor: It is currently too complicated to compute
    #     # We must search in current partitions to get an partition object with .hypopg_oid
    #     result = None
    #     for i in self.current_partitions:
    #         if partition == i:
    #             result = i
    #             break
    #     if result:
    #         # Partition does currently exist and size can be queried
    #         if not partition.estimated_size:
    #             partition.estimated_size = self.what_if.estimate_partition_size(result.table_name)
    #     else:
    #         raise NotImplementedError("Partition does not exist and size cannot be queried.")
    #         #self._simulate_or_create_index(partition, store_size=True) # TODO

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

    def calculate_cost(self, workload, partitions):
        assert (
            self.completed is False
        ), "Cost Evaluation is completed and cannot be reused."
        total_cost = 0

        for query in workload.queries:
            self.cost_requests += 1
            cost = self._request_cache(query, partitions)
            total_cost += cost

        return total_cost

    def calculate_cost_and_plans(self, workload, partitions, store_size=False):
        assert (
            self.completed is False
        ), "Cost Evaluation is completed and cannot be reused."
        start_time = datetime.datetime.now()

        # self._prepare_cost_calculation(partitions, store_size=store_size)
        total_cost = 0
        plans = []
        costs = []
        for query in workload.queries:
            self.cost_requests += 1
            cost, plan = self._request_cache_plans(query, partitions)
            cost = self.estimate_cost(plan, partitions)
            total_cost += cost
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
            self.what_if.simulate_partition(partition)
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
            query_plan = self.db_connector.get_plan(query)
            cost = self.estimate_cost(query_plan, relevant_partitions)
            self.cache[(query, relevant_partitions)] = cost
            return cost

    def estimate_cost(self, query_plan, partitions):

        total_cost = query_plan["Total Cost"]

        if not partitions or len(partitions) == 0: # if there are no partitions, return the cost of the query
            return total_cost
        
        if "Plans" in query_plan: #if there are subplans, sum all their costs
            return sum([self.estimate_cost(plan, partitions) for plan in query_plan["Plans"]])

        
        if "Filter" not in  query_plan: # if there is no filter, it needs to scan the whole table
            return total_cost

        query_filter = query_plan["Filter"]

        intervals = self._request_cache_intervals(query_filter)
    
        relevant_partitions = [x for x in partitions if any([x.column.name==interval for interval in intervals])]

        if not relevant_partitions: # if there are no relevant partitions, return the cost of the query
            return total_cost

        # relevant_partitions.sort() 
        unique_relevant_columns = {}
        for partition in relevant_partitions:
            if partition.column not in unique_relevant_columns:
                unique_relevant_columns[partition.column] = [partition]
            else:
                unique_relevant_columns[partition.column].append(partition)

        for column in unique_relevant_columns:
            percentiles = self._request_cache_percentiles(column)
            percentiles = [p[0] for p in percentiles]

            if not percentiles:
                return total_cost

            partitions = unique_relevant_columns[column]
            partitions.sort()

            if column.is_date():
                return self.estimate_costs_date(unique_relevant_columns[column][0], total_cost, intervals[column.name])
            else:
                maximum_percentile_bound = 1
                minimum_percentile_bound = 0
                interval_value_min = None
                interval_value_max = None

                interval = intervals[column.name]
                if column.is_numeric():
                    if interval[0] is not None:
                        interval_value_min = decimal.Decimal(interval[0])
                    if interval[1] is not None:
                        interval_value_max = decimal.Decimal(interval[1])
                else:
                    if interval[0] is not None:
                        interval_value_min = interval[0]
                    if interval[1] is not None:
                        interval_value_max = interval[1]

                total_partitions = 1
                for partition in partitions:

                    value = partition.upper_bound_value(percentiles)
                    
                    if interval_value_min is None:
                        # < or <=
                        if interval_value_max <= value:
                            maximum_percentile_bound = partition.upper_bound
                            break
                        else:
                            total_partitions += 1
                    elif interval_value_max is None:
                        # > or >=
                        if interval_value_min <= value:
                            minimum_percentile_bound = partition.upper_bound
                        else:
                            total_partitions = len(partitions)-partitions.index(partition)
                            break
                    else:
                        if interval_value_min <= value and interval_value_max <= value:
                            maximum_percentile_bound = partition.upper_bound
                            break
                        else:
                            minimum_percentile_bound = partition.upper_bound
                return total_cost*(maximum_percentile_bound-minimum_percentile_bound)*total_partitions
                # elif op == "<":
                #     for partition in partitions:
                #         upper_bound_value = partition.upper_bound_value(percentiles)
                #         if upper_bound_value > second:
                #             return total_cost*partition.upper_bound
                #     return total_cost*(len(partitions)+1)

    def get_interval(self, expression, interval):
        op = expression[1]
        if op == "AND" or op == "and":
            *_, (e1,i1) = self.get_interval(expression[0], interval).items()
            *_, (e2,i2) = self.get_interval(expression[2], interval).items()
            if e1 == e2:
                interval[e1] = utils.intersect_intervals(i1, i2)
            else:
                interval[e1] = i1
                interval[e2] = i2
        elif op == "OR" or op == "or":
            *_, (e1,i1) = self.get_interval(expression[0], interval).items()
            *_, (e2,i2) = self.get_interval(expression[2], interval).items()
            if e1 == e2:
                interval[e1] = utils.union_intervals(i1, i2)
            else:
                interval[e1] = i1
                interval[e2] = i2
        else:
            (e,i) = self._interval(expression)
            interval[e.lower()]=i

        return interval
            
    def _interval(self, expression):
        op = expression[1]
        val = expression[2]
        if isinstance(val, str) and ((val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'"))):
            val = val[1:-1]
        if op == "=":
            return (expression[0], (val,val))
        elif op == "<" or op == "<=":
            return (expression[0], (None,val))
        elif op == ">" or op == ">=":
            return (expression[0], (val, None))
        else:
            raise NotImplementedError(f"Operator not supported - {op}")

    def estimate_costs_date(self, partition, total_cost, interval):
        (minimum, maximum) = interval
        minimum = datetime.datetime.strptime(minimum, "%Y-%m-%d")
        maximum = datetime.datetime.strptime(maximum, "%Y-%m-%d")

        if partition.partition_rate == "daily":
            difference = (maximum - minimum)
            if difference.days == 0:
                return total_cost/30
            else:
                return total_cost*difference.days/30
        elif partition.partition_rate == "weekly":
            monday1 = minimum - datetime.timedelta(days=minimum.weekday())
            monday2 = maximum - datetime.timedelta(days=maximum.weekday())
            difference = (monday2 - monday1).days / 7
            if difference == 0:
                return total_cost/10
            else:
                return total_cost*difference/10
        elif partition.partition_rate == "monthly":
            difference = maximum.month - minimum.month
            if difference == 0:
                return total_cost/5
            else:
                return total_cost*difference/5
        elif partition.partition_rate == "yearly":
            difference = maximum.year - minimum.year
            if difference == 0:
                return total_cost/2
            else:
                return total_cost*difference/2

    def sanitize(self, value):
        value = value.split("::")[0]
        value = value.replace("'","")
        # remove leading and trailing whitespaces
        value = value.strip()
        return value

    def _request_cache_percentiles(self, column):
        if column in self.cache_percentiles:
            return self.cache_percentiles[column]
        else:
            percentiles = self.db_connector.get_column_percentiles(column)
            self.cache_percentiles[column] = percentiles
            return percentiles
        
    def _request_cache_intervals(self, query_filter):
        if query_filter in self.cache_intervals:
            return self.cache_intervals[query_filter]
        else:
            parsed_expression = self.expression_parser.parse(query_filter)[0]
            intervals = self.get_interval(parsed_expression.asList(), {})
            self.cache_intervals[query_filter] = intervals
            return intervals

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
            x for x in partitions if x.column in query.columns
        ]
        return frozenset(relevant_partitions)
