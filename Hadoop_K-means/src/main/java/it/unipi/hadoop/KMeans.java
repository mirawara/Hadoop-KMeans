package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;

//TODO: gestione delle eccezioni - Manca solo quella causata da Mergeoutput che deve essere tolta
public class KMeans {
	
	private static void KMeansIterations(Configuration conf, Path outputPath, Path inputPath, int numReducers) throws IOException {
		
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
			
			try {
				fs.delete(outputPath, true);
			} catch (IOException e) {
				System.err.println("Error during the deletion of the output file: " + e.getMessage());
				System.exit(1);
			}
			
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
			
			
			//TODO: non conviene fare il merge ma utilizzare un metodo per leggere i file dei reducer (più efficiente e anche più semplice)
			if (numReducers > 1) {
				KMeansUtil.mergeOutput(conf, outputPath, iteration);
			}
			
			// Read current centroids
			String currentCentroidFile = outputPath + "/part-r-00000";
			
			ArrayList<Centroid> currentCentroids = null;
			try {
				currentCentroids = KMeansUtil.readCentroids(currentCentroidFile, conf, false);
			} catch (IOException e) {
				System.err.println("Error during the reading of the current centroids: " + e.getMessage());
				System.exit(1);
			}
			double shift = KMeansUtil.calculateCentroidShift(currentCentroids, conf);
			
			converged = (shift < KMeansUtil.DEFAULT_THRESHOLD);
			if (!converged) {
				KMeansUtil.setCentroidsToConf("centroids", currentCentroids, conf);
			}
			
			try {
				KMeansUtil.logIterationInfo(iteration, shift);
			} catch (IOException e) {
				System.err.println("Error during the write on the MapReduce log file: " + e.getMessage());
				e.printStackTrace();
			}
			
			iteration++;
		}
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length < 3 || args.length > 4) {
			System.err.println("Usage: KMeansMain <inputPath> <outputPath> <centroidPath> [<numReducers>]");
			System.exit(1);
		}
		
		Path inputPath = null;
		Path outputPath = null;
		String centroidPath = null;
		int numReducers = 1;
		
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
		
		// Set centroids in the configuration
		try {
			KMeansUtil.setCentroidsToConf("centroids", KMeansUtil.readCentroids(centroidPath, conf, true), conf);
		} catch (IOException e) {
			System.err.println("Error during the reading of the centroids: " + e.getMessage());
			System.exit(1);
		}
		
		KMeansIterations(conf, outputPath, inputPath, numReducers);
		
	}
}
