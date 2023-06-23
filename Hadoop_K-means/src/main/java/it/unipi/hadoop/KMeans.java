package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A class that implements the K-means algorithm using Hadoop MapReduce.
 */
public class KMeans {

	/**
	 * Performs iterations of the K-means algorithm using the specified configuration, input path,
	 * number of reducers, and output path.
	 *
	 * @param conf         The Hadoop configuration.
	 * @param outputPath   The output path for storing the results.
	 * @param inputPath    The input path containing the data points.
	 * @param numReducers  The number of reducers to use in the MapReduce job.
	 */
	private static void KMeansIterations(Configuration conf, Path outputPath, Path inputPath, int numReducers) {
		// Get configuration file
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			System.err.println("Error during configuration file reading: " + e.getMessage());
			System.exit(1);
		}
		
		boolean converged = false;
		int iteration = 0;
		
		while (!converged && iteration < KMeansUtil.DEFAULT_MAX_ITERATIONS) {
			// Delete output path if it already exists
			try {
				fs.delete(outputPath, true);
			} catch (IOException e) {
				System.err.println("Error during the deletion of the output file: " + e.getMessage());
				System.exit(1);
			}
			
			// Job submission
			try (Job job = KMeansUtil.configureJob(conf, inputPath, outputPath, numReducers, iteration)) {
				if (job == null) {
					System.err.println("Error in Job configuration");
					System.exit(1);
				}
				if (!job.waitForCompletion(true)) {
					System.err.println("Error during Job execution");
					System.exit(1);
				}
				
			} catch (IOException | InterruptedException | ClassNotFoundException e) {
				e.printStackTrace();
			}
			
			// Read the current and previous centroids
			ArrayList<Centroid> currentCentroids = null;
			try {
				if (numReducers > 1) {
					currentCentroids = KMeansUtil.readCentroidFromMultipleFiles(conf, outputPath);
				} else {
					String currentCentroidFile = outputPath + "/part-r-00000";
					currentCentroids = KMeansUtil.readCentroids(currentCentroidFile, conf, false);
				}
			} catch (IOException e) {
				System.err.println("Error during the reading of the current centroids: " + e.getMessage());
				System.exit(1);
			}
			
			// Calculate the shift
			double shift = KMeansUtil.calculateCentroidShift(currentCentroids, conf);
			
			// Check if converged
			converged = (shift < KMeansUtil.DEFAULT_THRESHOLD);
			
			// Log the status
			KMeansUtil.logIterationInfo(iteration, shift, numReducers);
			if (!converged) {
				KMeansUtil.setCentroidsToConf("centroids", currentCentroids, conf);
			}
			
			iteration++;
		}
	}
	
	
	/**
	 * The main entry point for the K-means program.
	 *
	 * @param args The command-line arguments. Expects <inputPath> <outputPath> <centroidPath> [<numReducers>].
	 */
	public static void main(String[] args) {
		// Check if the number of arguments is valid
		if (args.length < 3 || args.length > 4) {
			System.err.println("Usage: KMeansMain <inputPath> <outputPath> <centroidPath> [<numReducers>]");
			System.exit(1);
		}
		
		Path inputPath = null;
		Path outputPath = null;
		String centroidPath = null;
		int numReducers = 1;
		
		// Parse the arguments
		try {
			inputPath = new Path(args[0]);
			outputPath = new Path(args[1]);
			centroidPath = args[2];
			numReducers = (args.length == 4) ? Integer.parseInt(args[3]) : KMeansUtil.DEFAULT_NUM_REDUCERS;
		} catch (IllegalArgumentException e) {
			System.err.println("Error during the parsing of the arguments: " + e.getMessage());
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		
		// Set initial centroids in the configuration
		try {
			KMeansUtil.setCentroidsToConf("centroids", KMeansUtil.readCentroids(centroidPath, conf, true), conf);
		} catch (IOException e) {
			System.err.println("Error during the reading of the centroids: " + e.getMessage());
			System.exit(1);
		}
		
		// Run KMeans iterations
		KMeansIterations(conf, outputPath, inputPath, numReducers);
	}
}
