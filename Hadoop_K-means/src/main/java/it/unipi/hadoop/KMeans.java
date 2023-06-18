package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class KMeans {

    /**
     * Extract the centroids from file
     *
     * @param path path to the file that contains the centroids
     * @return ArrayList of Centroid objects
     * @throws IOException if an I/O error occurs during file reading
     */
    public static ArrayList<Centroid> readCentroids(String path) throws IOException {
        ArrayList<Centroid> centroids = new ArrayList<>();
        DataInputStream in = new DataInputStream(Files.newInputStream(Paths.get(path)));
        while (in.available() > 0) {
            // Create a new Centroid object
            Centroid centroid = new Centroid();
            // Deserialize the centroid from the input stream
            centroid.readFields(in);
            centroids.set(centroid.getCentroid_id().get(), centroid);
        }
        in.close();
        return centroids;
    }
    public static ArrayList<Centroid> readCentroids(String pathString, Configuration conf) throws IOException {
        ArrayList<Centroid> centroids = new ArrayList<>();
        Path path = new Path(pathString);
        FileSystem hdfs = FileSystem.get(conf);
        FSDataInputStream in = hdfs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;

        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");

            // Creare un nuovo oggetto Centroid
            Centroid centroid = new Centroid();

            // Impostare l'ID del centroide
            int centroidId = (int) Double.parseDouble(fields[0]);
            centroid.getCentroid_id().set( centroidId);

            ArrayList<Double> coords =new ArrayList<>();
            for (int i = 1; i < fields.length-1; i++) {
                double value = Double.parseDouble(fields[i]);
                coords.add(value);
            }
            Point point = new Point(coords, Integer.parseInt(fields[fields.length-1]));
            centroid.setPoint(point);
            centroids.add(centroidId,centroid);
        }

        in.close();
        Collections.sort(centroids);
        return centroids;
    }



    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage: KMeansMain <inputPath> <outputPath> <centroidPath> [<numReducers>]");
            System.exit(1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String centroidPath = args[2];

        int numReducers = (args.length == 4) ? Integer.parseInt(args[3]) : KMeansUtil.DEFAULT_NUM_REDUCERS;


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        // Set centroids in the configuration

        //[centroid.getPoint().toString() for centroid in centroids]
        //Per ogni centroide nella lista di centroidi mi prendo la stringa delle coordinate del punto
        conf.setStrings("centroids", readCentroids(centroidPath,conf).stream()
                .map(centroid -> centroid.getPoint().toString())
                .toArray(String[]::new));

        ArrayList<Centroid> centroids2=new ArrayList<>();
        String[] centroidStrings = conf.getStrings("centroids");
        // Get the values of the centroids
        for (int i = 0; i < centroidStrings.length; i++) {
            System.out.println(centroidStrings[i]);
            centroids2.add(new Centroid(i, Arrays.stream(centroidStrings[i].split(" "))
                    .map(Double::parseDouble)
                    .collect(Collectors.toCollection(ArrayList::new))));
        }

        boolean converged = false;
        int iteration = 0;

        fs.delete(outputPath, true);
        while (!converged && iteration < KMeansUtil.DEFAULT_MAX_ITERATIONS) {

            Job job = KMeansUtil.configureJob(conf, inputPath, outputPath, numReducers, iteration);
            if (job == null) {
                System.err.println("Error in Job configuration");
                System.exit(1);
            }

            if (!job.waitForCompletion(true)) {
                System.err.println("Error during Job execution");
                System.exit(1);
            }
            System.exit(1);

            if (numReducers > 1) {
                KMeansUtil.mergeOutput(conf, outputPath, iteration);
            } else {
                fs.rename(new Path(outputPath, "part-00000"), new Path(outputPath, KMeansUtil.OUTPUT_NAME + iteration));
            }
            // Check convergence
            if (iteration > 0) {
                centroidPath = outputPath + KMeansUtil.OUTPUT_NAME + (iteration - 1);
            }
            // Read current centroids
            String currentCentroidFile = outputPath + KMeansUtil.OUTPUT_NAME + iteration;
            ArrayList<Centroid> currentCentroids=readCentroids(currentCentroidFile);
            double shift = KMeansUtil.calculateCentroidShift(centroidPath, currentCentroids);
            converged = (shift < KMeansUtil.DEFAULT_THRESHOLD);
            if (!converged) {
                conf.setStrings("centroids", readCentroids(currentCentroidFile).stream()
                        .map(centroid -> centroid.getPoint().toString())
                        .toArray(String[]::new));
            }
            iteration++;
        }

        KMeansUtil.cleanup(iteration, fs, outputPath);

    }


}
