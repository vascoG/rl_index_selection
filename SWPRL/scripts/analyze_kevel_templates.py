import csv
import sys
import re
from collections import defaultdict

def normalize_query(query):
    return re.sub(r'\$\d+', '?', query)


if __name__ == '__main__':
    csv.field_size_limit(sys.maxsize)
    data = []
    with open('pg_stat_statements_all.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            data.append({
                'id': row[0],
                'query': row[4],
                'calls': int(row[11]),
                'total_time': float(row[12]),
                'rows': int(row[17])
            })

    query_stats = defaultdict(lambda: {'calls': 0, 'total_time': 0, 'rows': 0})

    # Aggregate the data
    for row in data:
        query_template = normalize_query(row['query'])
        query_stats[query_template]['calls'] += row['calls']
        query_stats[query_template]['total_time'] += row['total_time']
        query_stats[query_template]['rows'] += row['rows']

    # Sort by the number of calls
    sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['total_time'], reverse=True)
    sorted_queries = sorted_queries[:10]
    total_time = sum([stats['total_time'] for query_template, stats in sorted_queries])

    # Display the most frequently executed queries
    print("Most Frequently Executed Queries:")
    for query_template, stats in sorted_queries:
        print(f"Query: {query_template}")
        print(f"  Calls: {stats['calls']}")
        print(f"  Total Time: {stats['total_time']}")
        print(f"  Rows: {stats['rows']}")
        print(f"  Percentage of total time: {stats['total_time'] / total_time * 100:.2f}%\n")
