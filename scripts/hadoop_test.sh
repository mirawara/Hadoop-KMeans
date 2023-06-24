# Check the argument
if [ $# -eq 0 ]; then
    echo "Please provide the name of a directory as the first argument."
    exit 1
fi

num_reducers="$1"

#for i in $(seq 7 7)
#do
#    test="test$i"
#    ./hadoop_run.sh $test $num_reducers
#done


./hadoop_run.sh test1 $num_reducers
./hadoop_run.sh test2 $num_reducers
./hadoop_run.sh test3 $num_reducers
./hadoop_run.sh test4 $num_reducers
./hadoop_run.sh test6 $num_reducers