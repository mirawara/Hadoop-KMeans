package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Utility class for the KMeans algorithm.
 * Provides various helper methods for reading centroids, configuring jobs, calculating centroid shifts, etc.
 */
public class KMeansUtil {

    public static final int DEFAULT_NUM_REDUCERS = 1;
    public static final double DEFAULT_THRESHOLD = 0.001;
    public static final int DEFAULT_MAX_ITERATIONS = 20;

    /**
     * Generates a Centroid object from an array of strings.
     *
     * @param fields The array of strings to be parsed.
     * @return The generated Centroid object.
     */
    private static Centroid generateCentroidFromArray(String[] fields) {
        Centroid centroid = new Centroid();

        // Set centroid ID
        int centroidId = (int) Double.parseDouble(fields[0]);
        centroid.getCentroid_id().set(centroidId);
        
        ArrayList<Double> coords = new ArrayList<>();
        for (int i = 1; i < fields.length; i++) {
            double value = Double.parseDouble(fields[i]);
            coords.add(value);
        }
        Point point = new Point(coords);
        centroid.setPoint(point);
        return centroid;
    }

    /**
     * Reads centroids from multiple files in a directory.
     *
     * @param conf    The Hadoop configuration.
     * @param dirPath The directory path containing the centroid files.
     * @return An ArrayList of Centroid objects read from the files.
     * @throws IOException If an I/O error occurs during file reading.
     */
    public static ArrayList<Centroid> readCentroidFromMultipleFiles(Configuration conf, Path dirPath) throws IOException {

        ArrayList<Centroid> centroids = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(dirPath);
        for (FileStatus status : fileStatus) {
            Path filePath = status.getPath();

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
            String line;
            while ((line = br.readLine()) != null) {

                String[] fields = line.split("\\s");
                centroids.add(generateCentroidFromArray(fields));
            }

        }
        Collections.sort(centroids);
        return centroids;
    }


    /**
     * Reads centroids from a file.
     *
     * @param pathString The path to the file that contains the centroids.
     * @param conf       The Hadoop configuration.
     * @param csvReading Flag indicating whether the file is in CSV format.
     * @return An ArrayList of Centroid objects read from the file.
     * @throws IOException If an I/O error occurs during file reading.
     */
    public static ArrayList<Centroid> readCentroids(String pathString, Configuration conf, boolean csvReading) throws IOException {
        ArrayList<Centroid> centroids = new ArrayList<>();
        Path path = new Path(pathString);
        FileSystem hdfs = FileSystem.get(conf);
        FSDataInputStream in = hdfs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;

        while ((line = reader.readLine()) != null) {

            String[] fields = line.split("\\s");

            if (csvReading) {
                fields = line.split(",");
            }
            Centroid centroid = generateCentroidFromArray(fields);
            centroids.add(centroid.getCentroid_id().get(), centroid);

        }

        in.close();
        return centroids;
    }

    /**
     * Configures a MapReduce job for K-Means iteration.
     *
     * @param conf        The Hadoop configuration.
     * @param inputPath   The input path for the MapReduce job.
     * @param outputPath  The output path for the MapReduce job.
     * @param numReducers The number of reducers for the MapReduce job.
     * @param iteration   The current iteration number.
     * @return The configured MapReduce job.
     */
    public static Job configureJob(Configuration conf, Path inputPath, Path outputPath, int numReducers, int iteration) {
        Job job;
        try {
            job = Job.getInstance(conf, "K-Means Iteration " + iteration);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);
            job.setNumReduceTasks(numReducers);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return job;
    }

    /**
     * Reads centroids from the Hadoop configuration.
     *
     * @param conf The Hadoop configuration.
     * @return An ArrayList of Centroid objects read from the configuration.
     */
    public static ArrayList<Centroid> readCentroidsFromConfiguration(Configuration conf) {
        
        // Create an empty list to store the centroids
        ArrayList<Centroid> centroids = new ArrayList<>();
        
        // Get the string representations of the centroids from the configuration object
        String[] centroidStrings = conf.getStrings("centroids");
        // Iterate over all centroid strings
        for (int i = 0; i < centroidStrings.length; i++) {
            // Convert the string representation of the centroid to a list of doubles
            ArrayList<Double> coordinates = Arrays.stream(centroidStrings[i].split(" "))
                    .map(Double::parseDouble)
                    .collect(Collectors.toCollection(ArrayList::new));
            // Create a new Centroid object and add it to the list
            centroids.add(new Centroid(i, coordinates));
        }
        // Return the list of centroids
        return centroids;
    }

    /**
     * Calculates the shift of centroids between current and previous iterations.
     *
     * @param currentCentroids The centroids of the current iteration.
     * @param conf             The Hadoop configuration.
     * @return The calculated centroid shift.
     */
    public static double calculateCentroidShift(ArrayList<Centroid> currentCentroids, Configuration conf) {
        
        // Read previous centroids from the configuration object
        ArrayList<Centroid> previousCentroids = readCentroidsFromConfiguration(conf);
        
        // Initialize the variable to store the total shift of all centroids
        double shift = 0.0;
        
        // Iterate over all centroids
        for (int i = 0; i < previousCentroids.size(); i++) {
            // Get the coordinates of the previous and current centroid
            ArrayList<Double> previousCentroidCoord = previousCentroids.get(i).getPoint().getCoordinates();
            ArrayList<Double> currentCentroidCoord = currentCentroids.get(i).getPoint().getCoordinates();
            // Calculate the shift for each dimension and add it to the total shift
            for (int j = 0; j < previousCentroidCoord.size(); j++) {
                shift += Math.abs(currentCentroidCoord.get(j) - previousCentroidCoord.get(j));
            }
        }
        // Return the total shift of all centroids
        return shift;
    }

    /**
     * Sets the centroids to the Hadoop configuration.
     *
     * @param key       The key for storing the centroids in the configuration.
     * @param centroids The centroids to be set in the configuration.
     * @param conf      The Hadoop configuration.
     */
    public static void setCentroidsToConf(String key, ArrayList<Centroid> centroids, Configuration conf) {
        // Set the value of the configuration property specified by `key` to an array of strings
        // representing the points of all centroids in the `centroids` collection.
        conf.setStrings(key, centroids.stream()
                .map(centroid -> centroid.getPoint().toString())
                .toArray(String[]::new));
    }

    /**
     * Logs iteration information to a log file.
     *
     * @param iteration   The current iteration number.
     * @param shift       The centroid shift value.
     * @param numReducers The number of reducers used in the iteration.
     */
    public static void logIterationInfo(int iteration, double shift, int numReducers) {
        // Use try-with-resources to automatically close the FileWriter, BufferedWriter and PrintWriter
        try (FileWriter fw = new FileWriter("map_reduce_log.txt", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            // Write the iteration information to the log file
            out.println("Iteration: " + iteration + ", Shift Value: " + shift + ", Converge Threshold: " + DEFAULT_THRESHOLD + ", Reduce Number: "+numReducers);
        } catch (IOException e) {
            // Handle any IOException that may occur
            System.err.println("Error during the write on the MapReduce log file");
            e.printStackTrace();
        }
    }

}
