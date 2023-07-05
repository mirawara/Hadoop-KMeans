from sklearn.metrics import silhouette_score
import numpy as np
import csv
import sys
import os

test_path = sys.argv[1]
path = f'../data/{test_path}/'

# Leggi i dati dal file del dataset
with open(f'{path}/dataset_test.csv', 'r') as f:
    reader = csv.reader(f)
    dataset = np.array([row for row in reader], dtype=float)

# Leggi i centroidi dal file dei centroidi
with open(f'{path}/results.csv', 'r') as f:
    reader = csv.reader(f)
    centroids = np.array([row for row in reader], dtype=float)

# Assegna ogni punto al suo centroide più vicino
distances = np.sqrt(((dataset[:, np.newaxis, :] - centroids) ** 2).sum(axis=2))
cluster_labels = np.argmin(distances, axis=1)

# Calcola il Silhouette Score
score = silhouette_score(dataset, cluster_labels)
print(f'Silhouette Score: {score}')

with open(f'{path}/map_reduce_log.txt', 'a') as f:
    f.write('\n')
    f.write(f'Silhouette Score: {score}\n')