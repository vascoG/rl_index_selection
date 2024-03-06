import itertools

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


def predict_partitions_sizes(columns, database_name):
    connector = PostgresDatabaseConnector(database_name, autocommit=True)
    connector.drop_indexes()
    # connector.drop_partitions() - there are no partitions in the original databases

    cost_evaluation = CostEvaluation(connector)

    predicted_partitions_sizes = []

    parent_partitions_size_map = {}

    for column in columns:
        potential_partition = Partition(column)

        connector.get_column_statistics(potential_partition.column)

        cost_evaluation.what_if.simulate_partition(potential_partition, True)

        full_partition_size = potential_partition.estimated_size

        if full_partition_size >= 0:
            predicted_partitions_sizes.append(full_partition_size)
            cost_evaluation.what_if.drop_simulated_partition(potential_partition)

        parent_partitions_size_map[column] = full_partition_size

    return predicted_partitions_sizes

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
