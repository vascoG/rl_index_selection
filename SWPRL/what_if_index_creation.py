import logging


# Class that encapsulates simulated/WhatIf-Indexes.
# This is usually used by the CostEvaluation class and there should be no need
# to use it manually.
# Uses hypopg for postgreSQL
class WhatIfPartitionCreation:
    def __init__(self, db_connector):
        logging.debug("Init WhatIfPartitionCreation")

        self.db_connector = db_connector

    def simulate_partition(self, potential_partition, store_size=False):
        result = self.db_connector.simulate_partition(potential_partition)

        if store_size:
            potential_partition.estimated_size = self.estimate_partition_size(potential_partition.table_name)

    def drop_simulated_partition(self, partition):
        table_name = partition.table_name
        self.db_connector.drop_simulated_partition(table_name, partition)

    def all_simulated_partitions(self):
        statement = "select * from hypopg_table();"
        partitions = self.db_connector.exec_fetch(statement)
        return partitions

    def estimate_partition_size(self, tablename): # TODO : Check if it works
        statement = f"SELECT hypopg_relation_size(relid) FROM hypopg_table() WHERE tablename = '{tablename}';"
        result = self.db_connector.exec_fetch(statement)
        if not result:
            return -1

        #assert result > 0, "Hypothetical partition does not exist." # TODO: Why is this always 0?
        return result[0]

    # TODO: refactoring
    # This is never used, we keep it for debugging reasons.
    def index_names(self):
        indexes = self.all_simulated_indexes()

        # Apparently, x[1] is the index' name
        return [x[1] for x in indexes]

    def drop_all_simulated_partitions(self):
        self.db_connector.drop_partitions()
