from functools import total_ordering
from index_selection_evaluation.selection.workload import Column


@total_ordering
class Partition:
    def __init__(self, column, value=None):
        if not isinstance(column, Column):
            raise ValueError("Partition needs at least and at most 1 column (be of instance Column)")
        self.column = column
        self.table_name = column.table.name
        self.invalid = False
        self.hypopg_name = None
        if column.is_date():
            self.partition_rate = value
        else:
            self.upper_bound = value

    def upper_bound_value(self, percentiles):
        if self.upper_bound is None:
            return None
        return percentiles[int(self.upper_bound*10-1)]

    # Used to sort partitions
    def __lt__(self, other):
        if self.column.table != other.column.table:
            return self.column.table < other.column.table
        elif self.column != other.column:
            return self.column < other.column
        else:
            if self.column.is_date():
                return self.partition_rate < other.partition_rate
            else:
                return self.upper_bound < other.upper_bound

    def __repr__(self):
        if self.column.is_date():
            return f"P({self.column.table.name}.{self.column.name}) - {self.partition_rate}"
        else:
            return f"P({self.column.table.name}.{self.column.name}) - {self.upper_bound}"

    def __eq__(self, other):
        if not isinstance(other, Partition):
            return False
        
        if self.column.type != other.column.type:
            return False

        if self.column.is_date():
            return self.column == other.column and self.partition_rate == other.partition_rate
        
        return self.column == other.column and self.upper_bound == other.upper_bound

    def __hash__(self):
        return hash(self.column)

    def _column_names(self):
        return self.column.name

    def is_single_column(self):
        return True

    def table(self):
        assert (
            self.column.table is not None
        ), "Table should not be None to avoid false positive comparisons."
        return self.column.table
