package main.java.it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
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
import java.net.URI;
import java.util.ArrayList;


public class KMeans {
    private static final int DEFAULT_NUM_REDUCERS = 1;
    private static final double DEFAULT_THRESHOLD = 0.001;
    private static final int DEFAULT_MAX_ITERATIONS = 10;

    private static final String OUTPUT_NAME = "output_centroids";

    private static Job configureJob(Configuration conf, Path inputPath, Path outputPath, int numReducers, int iteration) {
        Job job;
        try {
            job = Job.getInstance(conf, "K-Means Iteration " + iteration);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setMapOutputKeyClass(Centroid.class);
            job.setMapOutputValueClass(Point.class);
            job.setNumReduceTasks(numReducers);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, inputPath);
            if (iteration > 0) {

                job.addCacheFile(URI.create(outputPath + OUTPUT_NAME));

                FileSystem.get(conf).delete(outputPath, true);
            } else {
                FileSystem.get(conf).delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return job;
    }

    private static void mergeOutput(Configuration conf, Path outputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        Path outputFile = new Path(outputPath, OUTPUT_NAME);
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

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage: KMeansMain <inputPath> <outputPath> <centroidPath> [<numReducers>]");
            System.exit(1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String centroidPath = args[2];

        int numReducers = (args.length == 4) ? Integer.parseInt(args[3]) : DEFAULT_NUM_REDUCERS;


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Set centroid path in the configuration
        conf.set("centroidPath", centroidPath);

        boolean converged = false;
        int iteration = 0;

        while (!converged && iteration < DEFAULT_MAX_ITERATIONS) {

            Job job = configureJob(conf, inputPath, outputPath, numReducers, iteration);
            if (job == null) {
                System.err.println("Error in Job configuration");
                System.exit(1);
            }

            if (!job.waitForCompletion(true)) {
                System.err.println("Error during Job execution");
                System.exit(1);
            }

            if (numReducers > 1) {
                mergeOutput(conf, outputPath);
            } else {
                fs.rename(new Path(outputPath, "part-00000"), new Path(outputPath, OUTPUT_NAME));
            }
            // Check convergence
            double shift = calculateCentroidShift(conf, centroidPath, outputPath, numReducers);
            converged = (shift < DEFAULT_THRESHOLD);

            iteration++;
        }

    }

    private static double calculateCentroidShift(Configuration conf, String centroidPath, Path outputPath, int numReducers)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);

        // Read previous centroids
        Path centroidFile = new Path(centroidPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centroidFile)));
        ArrayList<String> previousCentroids = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            previousCentroids.add(line);
        }
        reader.close();

        // Read current centroids
        Path currentCentroidFile = new Path(outputPath, OUTPUT_NAME);
        reader = new BufferedReader(new InputStreamReader(fs.open(currentCentroidFile)));
        ArrayList<String> currentCentroids = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            currentCentroids.add(line);
        }
        reader.close();

        // Calculate centroid shift
        double shift = 0.0;
        for (int i = 0; i < previousCentroids.size(); i++) {
            String prevCentroid = previousCentroids.get(i);
            String currCentroid = currentCentroids.get(i);
            // Assuming centroids are represented as strings in the format "x1,x2,x3,...,xn"
            String[] prevCoords = prevCentroid.split(",");
            String[] currCoords = currCentroid.split(",");
            for (int j = 0; j < prevCoords.length; j++) {
                double prevCoord = Double.parseDouble(prevCoords[j]);
                double currCoord = Double.parseDouble(currCoords[j]);
                shift += Math.abs(currCoord - prevCoord);
            }
        }

        fs.delete(currentCentroidFile, true);
        return shift;
    }


}
