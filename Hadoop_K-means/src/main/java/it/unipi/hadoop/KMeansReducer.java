package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer class for the KMeans algorithm.
 * This class calculates the new centroid points based on the partial sums of the points assigned to each centroid.
 */
public class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Text> {

    /**
     * Reduce method of the Reducer class.
     * Calculates the new centroid points by averaging the partial sums of the assigned points.
     *
     * @param centroidId  The ID of the centroid.
     * @param partialSums The partial sums of the points assigned to the centroid.
     * @param context     The context object for accessing Hadoop services.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the execution is interrupted.
     */
    @Override
    protected void reduce(IntWritable centroidId, Iterable<Point> partialSums, Context context) throws IOException, InterruptedException {
        final Iterator<Point> it = partialSums.iterator();
        Point nextCentroidPoint = it.next();

        // Iterate over the partial sums and add every coordinate of the points
        while (it.hasNext()) {
            nextCentroidPoint.add(it.next());
        }

        // Calculate the average of the coordinates to obtain the new centroid point
        nextCentroidPoint.average();

        // Emit the centroid ID and the string representation of the new centroid point
        context.write(centroidId, new Text(nextCentroidPoint.toString()));
    }
}
