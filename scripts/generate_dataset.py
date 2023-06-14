from sklearn.datasets import make_blobs
import numpy as np
import pandas as pd
from matplotlib import pyplot
from pandas import DataFrame
from pandas.plotting._matplotlib import scatter_matrix


dimension = 4        # d
samples = 5000       # n
centers = 6          # k

points, y = make_blobs(n_samples = samples, centers = centers, n_features = dimension, random_state = 1, cluster_std = 1.5, shuffle = True)

df = DataFrame(dict(a=points[:,0], b=points[:,1], c=points[:,2], d=points[:,3], label=y))

df.to_csv('dataset_1.csv')
