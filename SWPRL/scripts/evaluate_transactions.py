import psycopg2
import time
import matplotlib.pyplot as plt

def measure_transaction_time(database_name, statements):
    try:
        
        connection = psycopg2.connect("dbname={}".format(database_name))
        connection.autocommit = False
        cursor = connection.cursor()

        start_time = time.time()

        # Execute the transaction
        for statement in statements:
            cursor.execute(statement)
        connection.commit()

        end_time = time.time()
        execution_time = end_time - start_time
                
        return execution_time

    except Exception as e:
        print("Error executing transaction:", e)
    finally:
        connection.close()


if __name__ == '__main__':
    TRANSACTIONS_FILE = "./SWPRL/scripts/transactions.txt"
    DATABASE_NAME_BASE = "dbt1"
    DATABASE_NAME_PARTITIONED = "dbt2"

    # Read the transactions from the file
    transactions = []
    with open(TRANSACTIONS_FILE, "r") as f:
        t = []
        for line in f:
            if line.strip() == "":
                transactions.append(t)
                t = []
            else:
                t.append(line.strip())

    # Measure the time for each transaction
    execution_times_base = []
    execution_times_partitioned = []

    for _ in range(10):
        for t in transactions:
            exec_time_base = measure_transaction_time(DATABASE_NAME_BASE, t)
            exec_time_partitioned = measure_transaction_time(DATABASE_NAME_PARTITIONED, t)
            
            execution_times_base.append(exec_time_base)
            execution_times_partitioned.append(exec_time_partitioned)

    avg_time_base = sum(execution_times_base) / len(execution_times_base) if execution_times_base else float('inf')
    avg_time_partitioned = sum(execution_times_partitioned) / len(execution_times_partitioned) if execution_times_partitioned else float('inf')

    print(f"Average - Total cost base: {avg_time_base}")
    print(f"Average - Total cost partitioned: {avg_time_partitioned}")

    # Plot the average execution times
    labels = ['Base DB', 'Partitioned DB']
    average_times = [avg_time_base, avg_time_partitioned]

    plt.figure(figsize=(8, 6))
    plt.bar(labels, average_times, color=['blue', 'green'])

    plt.xlabel('Database Type')
    plt.ylabel('Average Execution Time (seconds)')
    plt.title('Average Transaction Execution Time Comparison')

    # Save the plot to a file
    plot_filename = 'average_execution_times.png'
    plt.savefig(plot_filename)
    print(f"Plot saved as {plot_filename}")

    # Plot the execution times as a box plot
    data = [execution_times_base, execution_times_partitioned]
    labels = ['Base DB', 'Partitioned DB']

    plt.figure(figsize=(10, 6))
    plt.boxplot(data, labels=labels)

    plt.xlabel('Database Type')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Transaction Execution Time Distribution')

    # Save the plot to a file
    plot_filename = 'execution_time_distribution.png'
    plt.savefig(plot_filename)
    print(f"Plot saved as {plot_filename}")

