# Check the argument
if [ $# -eq 0 ]; then
    echo "Please provide the name of a directory as the first argument."
    exit 1
fi

num_reducers="$1"

#./hadoop_run.sh test1 $num_reducers
#./hadoop_run.sh test1 $num_reducers
./hadoop_run.sh test3 $num_reducers
./hadoop_run.sh test4 $num_reducers
./hadoop_run.sh test5 $num_reducers
./hadoop_run.sh test6 $num_reducers
./hadoop_run.sh test7 $num_reducers
