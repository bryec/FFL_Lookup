import csv

def read_data_from_file(filename):
    data = []
    with open(filename, 'r') as f:
        reader = csv.reader(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        next(reader)  # skip header row
        for row in reader:
            data.append(row)
    return data

data = read_data_from_file('monthly_data.txt')

for row in data:
    print(row)
