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
        // Create a Point object from the input text
        Point point = new Point(value.toString());
        
        // Initialize variables to store the ID of the nearest centroid and the distance to it
        IntWritable centroid_id = null;
        double distanceFromCentroid = Double.MAX_VALUE;
        
        // Iterate over all centroids to find the nearest one
        for (Centroid centroid : centroids) {
            // Calculate the distance between the current centroid and the point
            double distance = centroid.getPoint().getDistance(point);
            // If this is the first centroid or if it is closer than the previous nearest centroid,
            // update the nearest centroid ID and distance
            if (centroid_id == null || distance < distanceFromCentroid) {
                centroid_id = centroid.getCentroid_id();
                distanceFromCentroid = distance;
            }
        }
        // Emit the ID of the nearest centroid and the point
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
        
        // Call the superclass setup method
        super.setup(context);
        // Load the centroids from the Hadoop configuration and store them in the class variable
        centroids = readCentroidsFromConfiguration(context.getConfiguration());
    }
    
}

