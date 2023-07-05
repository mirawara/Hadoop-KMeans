# K-Means-Clustering-Algorithm-using-Hadoop-MapReduce

<span align="left">
    <img src="https://www.vectorlogo.zone/logos/java/java-horizontal.svg" alt="image" width="90" height="40">
    <img src="https://www.vectorlogo.zone/logos/apache_hadoop/apache_hadoop-icon.svg" alt="image" width="40" height="40">
    <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="image" width="40" height="40">
</span>     

University project for **Cloud Computing** course (MSc Computer Engineering at University of Pisa, A.Y. 2022-23).  

The goal of this project is to develop and analyze an application, that implements the **K-Means clustering** algorithm by exploiting **Hadoop MapReduce Parallelization**.   

The K-means clustering algorithm divides a dataset of n observations, whose characteristics and attributes are similar, into k clusters.   

The dataset is generated using a Python script that use the *make blobs* module from *scikit-learn*. The dimension and the structure of the dataset relies on specific parameters: 
- **n**: number of points/observations;
- **k**: number of clusters;
- **d**: number of dimension of the points/observations;

The algorithm is tested and evaluated on seven different datasets, with variations in n, k and d.   

Developed in Java 11 with Hadoop 3.1.3 Framework.  
Hadoop is deployed on a cluster of virtual machines, one namenode and two datanodes.   

# Structure of the repository 

```
K-Means-Clustering-Algorithm-using-Hadoop-MapReduce
|
├── Hadoop_K-means
│   └── src
│       ├── java/it/unipi/hadoop
|       └── resources
|
├── scripts
│   ├── data
│   |   ├── test1
│   |   ├── test2
│   |   ├── test3
│   |   ├── test4
│   |   ├── test5
│   |   ├── test6
|   |   └── test7  
|   |
│   ├── hadoop_run_scripts
│   ├── dataset_generation_scripts
│   └── results_analysis_scripts
|
├── javadoc
|
└── docs 
```

## Authors
- [Fabrizio Lanzillo](https://github.com/FabrizioLanzillo)
- [Federico Montini](https://github.com/FedericoMontini98)
- [Lorenzo Mirabella](https://github.com/mirawara)
