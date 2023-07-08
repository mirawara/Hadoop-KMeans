# Check the argument
if [ $# -eq 0 ]; then
    echo "Please provide the name of a directory as the first argument."
    exit 1
fi

num_reducers="$1"

for i in $(seq 3 3)
do
    test="test$i"
    ./hadoop_run.sh $test $num_reducers
done
