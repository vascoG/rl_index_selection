import matplotlib.pyplot as plt


def extract_array_from_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Use regular expressions to find the array in the file content
    array_str = re.search(r'\[(.*?)\]', content).group(0)
    
    # Convert the string representation of the array to an actual list of floats
    array = eval(array_str)
    
    return array



if __name__ == "__main__":
    base_path = "SWPRL/charts/execution_times_base_106GB.txt"
    partitioned_path = "SWPRL/charts/execution_times_partitioned_106GB.txt"

    base_times = extract_array_from_file(base_path)
    partitioned_times = extract_array_from_file(partitioned_path)
    
    # Generate a sequence of indices for x-axis
    x_base = range(len(base_times))
    x_partitioned = range(len(partitioned_times))

    # Plotting the base times
    plt.figure(figsize=(10, 5))
    plt.plot(x_base, base_times, label='Base Execution Times', marker='o')
    
    # Plotting the partitioned times
    plt.plot(x_partitioned, partitioned_times, label='Partitioned Execution Times', marker='s')
    
    # Adding title and labels
    plt.title('Execution Times Comparison')
    plt.xlabel('Run Index')
    plt.ylabel('Execution Time (ms)')
    plt.legend()
    
    # Display the plot
    plt.show()