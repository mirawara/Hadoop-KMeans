# Check the argument
if [ $# -eq 0 ]; then
    echo "Please provide the name of a directory as the first argument."
    exit 1
fi

test="$1"
num_reducers="$2"

# Check if the dir exists
if [ ! -d "../data/$test" ]; then
    echo "The directory '$test' doesn't exist."
    exit 1
fi

# Set the values of d, n, and k based on the selected test
if [ "$test" = "test1" ]; then
    d=2
    n=200
    k=2
elif [ "$test" = "test2" ]; then
    d=3
    n=500
    k=2
elif [ "$test" = "test3" ]; then
    d=3
    n=2500
    k=3
elif [ "$test" = "test4" ]; then
    d=3
    n=5000
    k=4
elif [ "$test" = "test5" ]; then
    d=3
    n=10000
    k=5
elif [ "$test" = "test6" ]; then
    d=4
    n=10000
    k=5
elif [ "$test" = "test7" ]; then
    d=15
    n=50000
    k=10
else
    echo "Invalid test value."
    exit 1
fi

# generate dataset for the specific test
python ../dataset_generation_scripts/generate_dataset.py $n $d $k $test

# move dataset on the remote machine 
scp ../data/$test/dataset_test.csv hadoop@hadoop-namenode:~/data/
scp ../data/$test/centroids_test.csv hadoop@hadoop-namenode:~/data/

# put the dataset on hdfs
ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop fs -put -f data/centroids_test.csv kmeansinput/centroids_test.csv"
ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop fs -put -f data/dataset_test.csv kmeansinput/dataset_test.csv"

# start of the mapreduce
ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop jar Hadoop_K-means-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeans kmeansinput/dataset_test.csv output/ kmeansinput/centroids_test.csv $num_reducers"

# get of the log from the remote machine
scp hadoop@hadoop-namenode:~/map_reduce_log.txt ../data/$test/map_reduce_log.txt 
ssh hadoop@hadoop-namenode "rm map_reduce_log.txt"

# get of the output files from the remote machine
if [ $num_reducers -gt 0 ]; then
    ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop fs -get /user/hadoop/output/part-r-00000 output"
    scp hadoop@hadoop-namenode:~/output/part-r-00000 ../data/$test/part-r-00000
    ssh hadoop@hadoop-namenode "rm output/part-r-00000"

fi

if [ $num_reducers -gt 1 ]; then
    ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop fs -get /user/hadoop/output/part-r-00001 output"
    scp hadoop@hadoop-namenode:~/output/part-r-00001 ../data/$test/part-r-00001 
    ssh hadoop@hadoop-namenode "rm output/part-r-00001"
fi

if [ $num_reducers -gt 2 ]; then
    ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop fs -get /user/hadoop/output/part-r-00002 output"
    scp hadoop@hadoop-namenode:~/output/part-r-00002 ../data/$test/part-r-00002
    ssh hadoop@hadoop-namenode "rm output/part-r-00002"
fi

python ../results_analysis_scripts/convert_to_csv.py $test
python ../results_analysis_scripts/compute_silhouette_index.py $test