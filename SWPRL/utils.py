import itertools
import random
import json
from decimal import Decimal

from SWPRL.cost_evaluation import CostEvaluation
from index_selection_evaluation.selection.dbms.postgres_dbms import PostgresDatabaseConnector
from SWPRL.partition import Partition


# Todo: This could be improved by passing index candidates as input
# def predict_index_sizes(column_combinations, database_name):
#     connector = PostgresDatabaseConnector(database_name, autocommit=True)
#     connector.drop_indexes()

#     cost_evaluation = CostEvaluation(connector)

#     predicted_index_sizes = []

#     parent_index_size_map = {}

#     for column_combination in column_combinations:
#         potential_index = Index(column_combination)
#         cost_evaluation.what_if.simulate_index(potential_index, True)

#         full_index_size = potential_index.estimated_size
#         index_delta_size = full_index_size
#         if len(column_combination) > 1:
#             index_delta_size -= parent_index_size_map[column_combination[:-1]]

#         predicted_index_sizes.append(index_delta_size)
#         cost_evaluation.what_if.drop_simulated_index(potential_index)

#         parent_index_size_map[column_combination] = full_index_size

#     return predicted_index_sizes


# def predict_partitions_sizes(columns, database_name):
#     connector = PostgresDatabaseConnector(database_name, autocommit=True)
#     connector.drop_indexes()
#     # connector.drop_partitions() - there are no partitions in the original databases

#     cost_evaluation = CostEvaluation(connector)

#     predicted_partitions_sizes = []

#     parent_partitions_size_map = {}

#     for column in columns:
#         potential_partition = Partition(column)

#         connector.get_column_statistics(potential_partition.column)

#         potential_partition.estimated_size = random.randint(1, 1000) # random number for now

#         # cost_evaluation.what_if.simulate_partition(potential_partition, True)

#         full_partition_size = potential_partition.estimated_size

#         # if full_partition_size >= 0:
#         predicted_partitions_sizes.append(full_partition_size)
#         #     cost_evaluation.what_if.drop_simulated_partition(potential_partition)

#         parent_partitions_size_map[column] = full_partition_size

#     return predicted_partitions_sizes

# def create_column_permutation_indexes(columns, max_index_width):
#     result_column_combinations = []

#     table_column_dict = {}
#     for column in columns:
#         if column.table not in table_column_dict:
#             table_column_dict[column.table] = set()
#         table_column_dict[column.table].add(column)

#     for length in range(1, max_index_width + 1):
#         unique = set()
#         count = 0
#         for key, columns_per_table in table_column_dict.items():
#             unique |= set(itertools.permutations(columns_per_table, length))
#             count += len(set(itertools.permutations(columns_per_table, length)))
#         print(f"{length}-column indexes: {count}")

#         result_column_combinations.append(list(unique))

#     return result_column_combinations

def all_partitions_from_columns(columns):
    print(f"Generating all partitions for {len(columns)} tables.")

    partitions = []
    for table in columns:
        tables = []
        for column in table:
            columns = []
            if column.is_date():
                for partition_rate in ["daily", "weekly", "monthly", "yearly"]:
                    columns.append(Partition(column, partition_rate))
            else:
                for upper_bound in range(1,10):
                    columns.append(Partition(column, upper_bound/10))
            columns.append(Partition(column, no_more_partitions=True))
            tables.append(columns)
        partitions.append(tables)
    
    return partitions


def intersect_intervals(interval1, interval2):
    if interval1[0] is None:
        minimum = interval2[0]
    elif interval2[0] is None:
        minimum = interval1[0]
    else:
        minimum = max(interval1[0], interval2[0])
    
    if interval1[1] is None:
        maximum = interval2[1]
    elif interval2[1] is None:
        maximum = interval1[1]
    else:
        maximum = min(interval1[1], interval2[1])
    
    # if interval1[1] < interval2[0] or interval2[1] < interval1[0]:
    #     return None
    return (minimum, maximum)

def union_intervals(interval1, interval2):
    if interval1[0] is None or interval2[0] is None:
        minimum = None
    else:
        minimum = min(interval1[0], interval2[0])
    
    if interval1[1] is None or interval2[1] is None:
        maximum = None
    else:
        maximum = max(interval1[1], interval2[1])

    return (minimum, maximum)


def output_partitions(partitions, database_name, path):
    connector = PostgresDatabaseConnector(database_name, autocommit=True)
    cost_evaluation = CostEvaluation(connector)
    print("\n------------------------------------\n\nRecommendations:")
    partitions_by_table = {}
    for p in partitions:
        (partition,reward) = p
        # if reward < 0:
        #     continue
        if partition.table_name not in partitions_by_table:
            partitions_by_table[partition.table_name] = [(partition, reward)]
        else:
            partitions_by_table[partition.table_name].append((partition, reward))
    final_partitions = {}    
    for table in partitions_by_table:
        print(f"\nPartitions on the Table \"{table}\":")
        minimum = 0
        maximum = 1
        partitions_by_table[table].sort()
        recommended_partitions = partitions_by_table[table]
        if not recommended_partitions[0][0].column.is_date():
            percentiles = cost_evaluation._request_cache_percentiles(recommended_partitions[0][0].column)
            percentiles = [p[0] for p in percentiles]
            len_percentiles = len(percentiles)
        for p in recommended_partitions:
            (partition,reward) = p
            if partition.no_more_partitions:
                continue
            if table not in final_partitions:
                final_partitions[table] = []
            if partition.column.is_date():
                print(f"Partition on Column \"{partition.column.name}\" at a {partition.partition_rate} rate - {reward}")
                final_partitions[table].append({'column': partition.column.name, 'partition_rate': partition.partition_rate, 'type': 'date'})
            else:
                if minimum == 0:
                    print(f"Partition on Column \"{partition.column.name}\" with values lower than {partition.upper_bound_value(percentiles)} - {reward}")
                    final_partitions[table].append({'column': partition.column.name, 'upper_bound': partition.upper_bound_value(percentiles), 'type': 'range'})
                    minimum = partition.upper_bound
                    if recommended_partitions.index(p) == len(recommended_partitions)-1:
                        print(f"Partition on Column \"{partition.column.name}\" with values higher than {partition.upper_bound_value(percentiles)} - {reward}")
                        final_partitions[table].append({'column': partition.column.name, 'lower_bound': partition.upper_bound_value(percentiles), 'type': 'range'})
                else:
                    if recommended_partitions.index(p) == len(recommended_partitions)-1:
                        print(f"Partition on Column \"{partition.column.name}\" between {percentiles[(int(10*minimum-1))%len_percentiles]} and {partition.upper_bound_value(percentiles)} - {reward}")
                        print(f"Partition on Column \"{partition.column.name}\" with values higher than {partition.upper_bound_value(percentiles)} - {reward}")
                        final_partitions[table].append({'column': partition.column.name, 'lower_bound': percentiles[(int(10*minimum-1))%len_percentiles], 'upper_bound': partition.upper_bound_value(percentiles), 'type': 'range'})
                        final_partitions[table].append({'column': partition.column.name, 'lower_bound': partition.upper_bound_value(percentiles), 'type': 'range'})
                    else:   
                        print(f"Partition on Column \"{partition.column.name}\" between {percentiles[(int(10*minimum-1))%len_percentiles]} and {partition.upper_bound_value(percentiles)} - {reward}")
                        final_partitions[table].append({'column': partition.column.name, 'lower_bound': percentiles[(int(10*minimum-1))%len_percentiles], 'upper_bound': partition.upper_bound_value(percentiles), 'type': 'range'})
                        minimum = partition.upper_bound

    # convert any Decimal to float
    for table in final_partitions:
        for partition in final_partitions[table]:
            for key in partition:
                if isinstance(partition[key], Decimal):
                    partition[key] = float(partition[key])

    # output final_partitions to a file
    with open(f"{path}/final_partitions_all_rewards.json", "w") as f:
        json.dump(final_partitions, f)


