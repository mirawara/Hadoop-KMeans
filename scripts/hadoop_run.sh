# Check the argument
if [ $# -eq 0 ]; then
    echo "Please provide the name of a directory as the first argument."
    exit 1
fi

test="$1"
num_reducers="$2"

# Check if the dir exists
if [ ! -d "data/$test" ]; then
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
    d=30
    n=100000
    k=25
else
    echo "Invalid test value."
    exit 1
fi

python generate_dataset.py $n $d $k $test

scp data/$test/dataset_test.csv hadoop@hadoop-namenode:~/data/
scp data/$test/centroids_test.csv hadoop@hadoop-namenode:~/data/

ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop fs -put -f data/centroids_test.csv kmeansinput/centroids_test.csv"
ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop fs -put -f data/dataset_test.csv kmeansinput/dataset_test.csv"

ssh hadoop@hadoop-namenode "/opt/hadoop/bin/hadoop jar Hadoop_K-means-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeans kmeansinput/dataset_test.csv output/ kmeansinput/centroids_test.csv $num_reducers"

scp hadoop@hadoop-namenode:~/map_reduce_log.txt data/$test/map_reduce_log.txt 
ssh hadoop@hadoop-namenode "rm map_reduce_log.txt"
