import csv

def read_csv_file(file_path):
    """
    CBV: Reads a CSV file and prints its contents.

    Args:
        file_path (str): The path to the CSV file.
    """
    try:
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                print(row)
    except FileNotFoundError:
        print(f"Error: File not found at path: {file_path}")
    except Exception as e:
         print(f"An error occurred: {e}")

if __name__ == "__main__":
    file_path = 'info_rep1_curps_24abril2025.csv'
    # Create a dummy CSV file for demonstration
#    with open(file_path, 'w', newline='') as file:
#        writer = csv.writer(file)
#        writer.writerow(['Name', 'Age', 'City'])
#        writer.writerow(['Alice', '30', 'New York'])
#        writer.writerow(['Bob', '25', 'Los Angeles'])
    read_csv_file(file_path)
    