import csv
import sys
import os

test_path = sys.argv[1]
path = f'../data/{test_path}/'

with open(f'{path}/output.txt', 'w') as output_file:
    for filename in os.listdir(path):
        if 'part' in filename:
            with open(f'{path}/{filename}', 'r') as input_file:
                for line in input_file:
                    output_file.write(line)

with open(f'{path}/output.txt', 'r') as input_file:
    with open(f'{path}/results.csv', 'w') as output_file:
        writer = csv.writer(output_file)
        for line in input_file:
            data = line.strip().split()
            writer.writerow(data[1:])
