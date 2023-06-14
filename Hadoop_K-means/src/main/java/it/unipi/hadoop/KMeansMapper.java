package main.java.it.unipi.hadoop;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class KMeansMapper extends Mapper<Object, Text, Centroid, Point> {
	
	private static ArrayList<Centroid> centroids;
	
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
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		// Read centroids returned from previous iteration inside the distributed cache
		URI[] cacheFiles = context.getCacheFiles();
		if (cacheFiles != null && cacheFiles.length > 0) {
			// Read the centroids from the cached file
			centroids = readCentroids(cacheFiles[0].getPath());
			return;
		}
		// If we are in the first iteration no centroids will be found, get random ones
		super.setup(context);
		String inputPath = context.getConfiguration().get("centroids");
		centroids = readCentroids(inputPath);
	}
	
	/** Extract the centroids from file
	 * @param path path to the file that contains the centroids
	 * @return ArrayList of Centroid objects
	 * @throws IOException if an I/O error occurs during file reading
	 */
	private ArrayList<Centroid> readCentroids(String path) throws IOException {
		ArrayList<Centroid> centroids = new ArrayList<>();
		DataInputStream in = new DataInputStream(Files.newInputStream(Paths.get(path)));
		while (in.available() > 0) {
			// Create a new Centroid object
			Centroid centroid = new Centroid();
			// Deserialize the centroid from the input stream
			centroid.readFields(in);
			centroids.add(centroid);
		}
		in.close();
		return centroids;
	}
}

