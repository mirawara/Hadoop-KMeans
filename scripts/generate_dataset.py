from sklearn.datasets import make_blobs
import numpy as np
import pandas as pd
from matplotlib import pyplot
from pandas import DataFrame
from pandas.plotting._matplotlib import scatter_matrix
import random


dimension = 3        # d
samples = 10000        # n
centers = 3          # k

points, y = make_blobs(n_samples = samples, centers = centers, n_features = dimension, random_state = 1, cluster_std = 1.5, shuffle = True)

df = DataFrame(dict(a=points[:,0], b=points[:,1], c=points[:,2]))

df = df.apply(lambda x: (x - x.min()) / (x.max() - x.min()))

df.to_csv('dataset_test.csv', index=False, header=False)


centroids = DataFrame(columns=["id", "a", "b", "c", "d"])
for i in range(0, centers):
  n = random.randint(0,samples)
  chosen_point = df.loc[n]
  new_row = {'id': i, 'a': chosen_point['a'],'b': chosen_point['b'], 'c': chosen_point['c']}
  centroids = centroids.append(new_row, ignore_index=True)


centroids.to_csv('centroids_test.csv', index = False, header=False)
