import sys
from sklearn.datasets import make_blobs
import pandas as pd
import random

if len(sys.argv) != 5:
    print("Usage: python script.py <n> <d> <k> <folder_name>")
    sys.exit(1)

samples = int(sys.argv[1])      # n
dimension = int(sys.argv[2])    # d
centers = int(sys.argv[3])      # k
folder_name = sys.argv[4]

points, y = make_blobs(n_samples=samples, centers=centers, n_features=dimension, random_state=1, cluster_std=1.2, shuffle=True)

df = pd.DataFrame(points)

df = df.apply(lambda x: (x - x.min()) / (x.max() - x.min()))

df.to_csv(f'data/{folder_name}/dataset_test.csv', index=False, header=False)

centroids = pd.DataFrame(columns=["id"] + list(range(dimension)))
for i in range(0, centers):
    n = random.randint(0, samples)
    chosen_point = df.loc[n]
    new_row = pd.DataFrame({'id': [i], **{j: [chosen_point[j]] for j in range(dimension)}})
    centroids = pd.concat([centroids, new_row], ignore_index=True)

centroids.to_csv(f'data/{folder_name}/centroids_test.csv', index=False, header=False)