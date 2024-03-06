from functools import total_ordering
from index_selection_evaluation.selection.workload import Column


@total_ordering
class Partition:
    def __init__(self, column, estimated_size=None):
        if not isinstance(column, Column):
            raise ValueError("Partition needs at least and at most 1 column (be of instance Column)")
        self.column = column
        self.table_name = column.table.name
        # Store hypopg estimated size when `store_size=True` (whatif)
        self.estimated_size = estimated_size
        self.hypopg_name = None

    # Used to sort partitions
    def __lt__(self, other):

        return self.column < other.column

    def __repr__(self):
        return f"P({self.column.table.name}.{self.column.name})"

    def __eq__(self, other):
        if not isinstance(other, Partition):
            return False

        return self.column == other.column

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
