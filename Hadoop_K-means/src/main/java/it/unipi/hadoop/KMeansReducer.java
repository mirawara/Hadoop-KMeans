package main.java.it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable centroidId, Iterable<Point> partialSums, Context context) throws IOException, InterruptedException {
        final Iterator<Point> it = partialSums.iterator();
        Point nextCentroidPoint = it.next();
        while (it.hasNext()) {
            // Add every coordinate of the points
            nextCentroidPoint.add(it.next());
        }
        nextCentroidPoint.average();
        context.write(centroidId, new Text(nextCentroidPoint.toString()));
    }
}