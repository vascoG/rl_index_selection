import logging


# Class that encapsulates simulated/WhatIf-Partitions.
# This is usually used by the CostEvaluation class and there should be no need
# to use it manually.
# Uses hypopg for postgreSQL
class WhatIfPartitionCreation:
    def __init__(self, db_connector):
        logging.debug("Init WhatIfPartitionCreation")

        self.db_connector = db_connector

    def simulate_partition(self, potential_partition, store_size=False):
        minimum = potential_partition.column.minimum
        maximum = potential_partition.column.maximum
        median = potential_partition.column.median

        if minimum is None or maximum is None or median is None:
            potential_partition.estimated_size = -1
            return
        if minimum == median or maximum == median:
            logging.info(f"This partition has no variance: {potential_partition.column}")
            potential_partition.estimated_size = -1
            return
        
        result = self.db_connector.simulate_partition(potential_partition)

        if store_size:
            potential_partition.estimated_size = self.estimate_partition_size(potential_partition.table_name)

    def drop_simulated_partition(self, partition):
        if partition.estimated_size < 0:
            return
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

        #assert result > 0, "Hypothetical partition does not exist." # TODO: Uncomment this if I ever implement partition size estimation
        return result[0]

    def drop_all_simulated_partitions(self):
        self.db_connector.drop_partitions()
