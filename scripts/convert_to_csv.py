import csv

with open('output.txt', 'r') as input_file:
    with open('results.csv', 'w') as output_file:
        writer = csv.writer(output_file)
        for line in input_file:
            data = line.strip().split()
            writer.writerow(data[1:])
