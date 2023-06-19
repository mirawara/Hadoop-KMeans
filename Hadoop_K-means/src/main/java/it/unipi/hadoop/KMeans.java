package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;


public class KMeans {

    /**
     * Extract the centroids from file
     *
     * @param pathString path to the file that contains the centroids
     * @return ArrayList of Centroid objects
     * @throws IOException if an I/O error occurs during file reading
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

            if(csvReading){
                fields = line.split(",");
            }

            // Creare un nuovo oggetto Centroid
            Centroid centroid = new Centroid();

            // Impostare l'ID del centroide
            int centroidId = (int) Double.parseDouble(fields[0]);
            centroid.getCentroid_id().set( centroidId);

            ArrayList<Double> coords =new ArrayList<>();
            for (int i = 1; i < fields.length; i++) {
                double value = Double.parseDouble(fields[i]);
                coords.add(value);
            }
            Point point = new Point(coords);
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

        boolean converged = false;
        int iteration = 0;

        // Set centroids in the configuration

        //[centroid.getPoint().toString() for centroid in centroids]
        //Per ogni centroide nella lista di centroidi mi prendo la stringa delle coordinate del punto
        conf.setStrings("centroids", readCentroids(centroidPath,conf, true).stream()
                .map(centroid -> centroid.getPoint().toString())
                .toArray(String[]::new));


        while (!converged && iteration < KMeansUtil.DEFAULT_MAX_ITERATIONS) {

            fs.delete(outputPath, true);

            try (Job job = KMeansUtil.configureJob(conf, inputPath, outputPath, numReducers, iteration)) {
                if (job == null) {
                    System.err.println("Error in Job configuration");
                    System.exit(1);
                }
                if (!job.waitForCompletion(true)) {
                    System.err.println("Error during Job execution");
                    System.exit(1);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            if (numReducers > 1) {
                KMeansUtil.mergeOutput(conf, outputPath, iteration);
            } else {
                fs.rename(new Path(outputPath + "/part-r-00000"), new Path(outputPath + KMeansUtil.OUTPUT_NAME + iteration));
            }

            // Read current centroids
            String currentCentroidFile = outputPath + KMeansUtil.OUTPUT_NAME + iteration;
            ArrayList<Centroid> currentCentroids=readCentroids(currentCentroidFile, conf, false);
            double shift = KMeansUtil.calculateCentroidShift(currentCentroids, conf);
            converged = (shift < KMeansUtil.DEFAULT_THRESHOLD);

            try (FileWriter fw = new FileWriter("map_reduce_log.txt", true);
                 BufferedWriter bw = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(bw)) {
                out.println("MapReduce Iteration: " + iteration + ", Shift Value: " + shift + ", Converge Threshold: " + KMeansUtil.DEFAULT_THRESHOLD);
            } catch (IOException e) {
                System.err.println("Error during the write on the MapReduce log file");
                e.printStackTrace();
            }

            if (!converged) {
                conf.setStrings("centroids", readCentroids(currentCentroidFile, conf, false).stream()
                        .map(centroid -> centroid.getPoint().toString())
                        .toArray(String[]::new));
            }
            iteration++;
        }
    }
}
