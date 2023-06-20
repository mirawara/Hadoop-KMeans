package it.unipi.hadoop;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

import static it.unipi.hadoop.KMeansUtil.readCentroidsFromConfiguration;

/**
 * Mapper class for the K-Means algorithm.
 * Maps each data point to its nearest centroid.
 */
public class KMeansMapper extends Mapper<Object, Text, IntWritable, Point> {

    private static ArrayList<Centroid> centroids;

    /**
     * Maps each data point to its nearest centroid.
     *
     * @param key     The input key.
     * @param value   The input value representing the coordinates of a data point.
     * @param context The context object for writing the centroid-point association.
     * @throws InterruptedException Thrown when a thread is interrupted while waiting, sleeping, or otherwise occupied.
     * @throws IOException          Thrown when an I/O error occurs.
     */
    protected void map(final Object key, final Text value, final Context context) throws InterruptedException, IOException {
        // Create new point from text
        Point point = new Point(value.toString());

        // Initialization
        IntWritable centroid_id = null;
        double distanceFromCentroid = Double.MAX_VALUE;

        // Scan for nearest centroid
        for (Centroid centroid : centroids) {
            // Calculate distance between current centroid and the point
            double distance = centroid.getPoint().getDistance(point);
            // If the current centroid is the first tested or is nearer than the previous ones, then save new ID and distance
            if (centroid_id == null || distance < distanceFromCentroid) {
                centroid_id = centroid.getCentroid_id();
                distanceFromCentroid = distance;
            }
        }
        // Centroid emit
        if (centroid_id == null) {
            System.out.println("Error: centroid_id is null");
            return;
        }
        context.write(centroid_id, point);
    }

    /**
     * Reads the centroids from the Hadoop configuration during the setup phase.
     *
     * @param context The context object for accessing the configuration.
     * @throws IOException          Thrown when an I/O error occurs.
     * @throws InterruptedException Thrown when a thread is interrupted while waiting, sleeping, or otherwise occupied.
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
        centroids = readCentroidsFromConfiguration(context.getConfiguration());
    }
}

