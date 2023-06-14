package main.java.it.unipi.hadoop;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

public class KMeansMapper extends Mapper<Object, Text, Centroid, Point> {
	
	private final static ArrayList<Centroid> centroids = new ArrayList<Centroid>(); // Centroid List
	private static int k; // Centroid number
	
	/**
	 * @param key
	 * @param value Coordinates of the point to map to a centroid
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
		for(Centroid centroid : centroids) {
			// Calculate distance between current centroid and the point
			double distance = centroid.getPoint().getDistance(point);
			// If the current centroid is the first tested or is nearer than the previous ones, then save new ID and distance
			if (centroid_id == null || distance < distanceFromCentroid){
				centroid_id = centroid.getCentroid_id();
				distanceFromCentroid = distance;
			}
		}
		// Centroid emit
		if(centroid_id == null){
			System.out.println("Error: centroid_id is null");
			return;
		}
		context.write(centroids.get(centroid_id.get()), point);
	}
	
	/**
	 * @param context Information about centroid-point association
	 * @throws IOException Thrown when an I/O error occurs during Cached file reading
	 * @throws InterruptedException thrown when a thread is interrupted while it's waiting, sleeping, or otherwise occupied
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Read centroids returned from previous iteration inside the distributed cache
		try {
			URI[] cacheFiles = context.getCacheFiles();
			if (cacheFiles != null && cacheFiles.length > 0) {
				// Read centroids from a file inside the distributed cache
				Path centroidPath = new Path(cacheFiles[0].toString());
				FileSystem fs = FileSystem.get(context.getConfiguration());
				try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidPath)))) {
					String line;
					while ((line = br.readLine()) != null) {
						// TODO: Parsa e aggiungi i centroidi alla lista
					}
				}
			}
		} catch (IOException e) {
			System.err.println("Error reading centroids from cache: " + e.getMessage());
		}
		
		// If we are in the first iteration no centroids will be found, initialize them randomly
		if (centroids.isEmpty()) {
			generate_random_centroids(context);
		}
	}
	
	private void generate_random_centroids(Context context){
		// TODO SET IN CONFIGURATION
		int num_coordinates = context.getConfiguration().getInt("num_coordinates", 10);
		int MAX_VALUE = context.getConfiguration().getInt("max_value", Integer.MAX_VALUE);
		
		// Choose k random indices without replacement
		for (int i = 0; i < k; i++) {
			//Generate randomly new coordinates
			ArrayList<Double> coordinates = new ArrayList<>();
			// Generate random value for each coordinate
			for(int n = 0; n < num_coordinates; n++ ) {
				coordinates.add(Math.random() * MAX_VALUE);
			}
			
			Point point = new Point(coordinates);
			//Add new element to ArrayList
			centroids.add(new Centroid());
			// Set new Point
			centroids.get(i).setPoint(point);
			// Set new centroid ID
			IntWritable new_id = new IntWritable(i);
			centroids.get(i).setCentroid_id(new_id);
		}
	}
}

