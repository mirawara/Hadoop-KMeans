package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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

public class KMeansUtil {

    public static final int DEFAULT_NUM_REDUCERS = 1;
    public static final double DEFAULT_THRESHOLD = 0.001;
    public static final int DEFAULT_MAX_ITERATIONS = 20;

    public static final String OUTPUT_NAME = "/output_centroids";


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

            if (csvReading) {
                fields = line.split(",");
            }

            // Creare un nuovo oggetto Centroid
            Centroid centroid = new Centroid();

            // Impostare l'ID del centroide
            int centroidId = (int) Double.parseDouble(fields[0]);
            centroid.getCentroid_id().set(centroidId);

            ArrayList<Double> coords = new ArrayList<>();
            for (int i = 1; i < fields.length; i++) {
                double value = Double.parseDouble(fields[i]);
                coords.add(value);
            }
            Point point = new Point(coords);
            centroid.setPoint(point);
            centroids.add(centroidId, centroid);
        }

        in.close();
        Collections.sort(centroids);
        return centroids;
    }

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

    public static void mergeOutput(Configuration conf, Path outputPath, int iteration) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        Path outputFile = new Path(outputPath, OUTPUT_NAME + iteration);
        OutputStream out = fs.create(outputFile);
        for (FileStatus fileStatus : fs.listStatus(outputPath)) {
            if (fileStatus.isFile() && !fileStatus.getPath().equals(outputFile)) {
                InputStream in = fs.open(fileStatus.getPath());
                IOUtils.copyBytes(in, out, conf);
                in.close();
            }
        }
        out.close();
    }

    public static ArrayList<Centroid> readCentroidsFromConfiguration(Configuration conf) {

        ArrayList<Centroid> centroids = new ArrayList<>();

        String[] centroidStrings = conf.getStrings("centroids");
        // Get the values of the centroids
        for (int i = 0; i < centroidStrings.length; i++) {
            centroids.add(new Centroid(i, Arrays.stream(centroidStrings[i].split(" "))
                    .map(Double::parseDouble)
                    .collect(Collectors.toCollection(ArrayList::new))));
        }
        return centroids;
    }

    public static double calculateCentroidShift(ArrayList<Centroid> currentCentroids, Configuration conf) {

        // Read previous centroids
        ArrayList<Centroid> previousCentroids = readCentroidsFromConfiguration(conf);

        // Calculate centroid shift
        double shift = 0.0;
        for (int i = 0; i < previousCentroids.size(); i++) {
            ArrayList<Double> previousCentroidCoord = previousCentroids.get(i).getPoint().getCoordinates();
            ArrayList<Double> currentCentroidCoord = currentCentroids.get(i).getPoint().getCoordinates();
            for (int j = 0; j < previousCentroidCoord.size(); j++) {
                shift += Math.abs(currentCentroidCoord.get(j) - previousCentroidCoord.get(j));
            }
        }
        return shift;
    }

    public static void setCentroidsToConf(String key, ArrayList<Centroid> centroids, Configuration conf){
        //[centroid.getPoint().toString() for centroid in centroids]
        //Per ogni centroide nella lista di centroidi mi prendo la stringa delle coordinate del punto
        conf.setStrings(key, centroids.stream()
                .map(centroid -> centroid.getPoint().toString())
                .toArray(String[]::new));
    }

    public static void logIterationInfo(int iteration, double shift) throws IOException {
        FileWriter fw = new FileWriter("map_reduce_log.txt", true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw);
        out.println("MapReduce Iteration: " + iteration + ", Shift Value: " + shift + ", Converge Threshold: " + DEFAULT_THRESHOLD);
    }

}
