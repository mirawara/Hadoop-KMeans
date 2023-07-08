import sys
from sklearn.datasets import make_blobs
import pandas as pd
from sklearn.cluster import KMeans

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

df.to_csv(f'../data/{folder_name}/dataset_test.csv', index=False, header=False)

centroids = df.sample(n=centers)
centroids.index = range(centers)

#kmeans = KMeans(n_clusters=centers, init='k-means++', n_init=1)
#kmeans.fit(df)
#centroids = pd.DataFrame(kmeans.cluster_centers_)
#centroids.index = range(centers)

centroids.to_csv(f'../data/{folder_name}/centroids_test.csv', index=True, header=False)
