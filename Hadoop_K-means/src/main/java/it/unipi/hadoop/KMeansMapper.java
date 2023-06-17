package it.unipi.hadoop;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Point> {

    private static ArrayList<Centroid> centroids;

    /**
     * @param key
     * @param value   Coordinates of the point to map to a centroid
     * @param context Information about centroid-point association
     * @throws InterruptedException thrown when a thread is interrupted while it's waiting, sleeping, or otherwise occupied
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
     * @param context Information about centroid-point association
     * @throws IOException Thrown when an I/O error occurs during Cached file reading
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        centroids=new ArrayList<>();
        super.setup(context);
        String[] centroidStrings = context.getConfiguration().getStrings("centroids");
        // Get the values of the centroids
        for (int i = 0; i < centroidStrings.length; i++) {
            centroids.add(new Centroid(i, Arrays.stream(centroidStrings[i].split(","))
                    .map(Double::parseDouble)
                    .collect(Collectors.toCollection(ArrayList::new))));
        }

    }
}

