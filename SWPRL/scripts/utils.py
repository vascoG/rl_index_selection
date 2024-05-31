import pandas as pd

if __name__ == '__main__':
    # Load the CSV file into a DataFrame
    df = pd.read_csv('sample2.csv', header=None)

    # Display the first few rows to understand the structure
    print(df.head())

    # Change the content of the specified columns to "A" and "B"
    # Replace 'column1' and 'column2' with the actual column names
    df[3] = 'source'
    df[4] = 'orders'

    # Save the modified DataFrame back to a CSV file
    df.to_csv('sample3.csv', index=False)
