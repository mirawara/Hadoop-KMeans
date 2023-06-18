package it.unipi.hadoop;

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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class KMeansUtil {

    public static final int DEFAULT_NUM_REDUCERS = 1;
    public static final double DEFAULT_THRESHOLD = 0.001;
    public static final int DEFAULT_MAX_ITERATIONS = 10;

    public static final String OUTPUT_NAME = "/output_centroids";



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

    public static ArrayList<Centroid> readCentroidsFromConfiguration(Configuration conf){

        ArrayList<Centroid> centroids=new ArrayList<>();

        String[] centroidStrings = conf.getStrings("centroids");
        // Get the values of the centroids
        for (int i = 0; i < centroidStrings.length; i++) {
            centroids.add(new Centroid(i, Arrays.stream(centroidStrings[i].split(" "))
                    .map(Double::parseDouble)
                    .collect(Collectors.toCollection(ArrayList::new))));
        }
        return centroids;
    }

    public static double calculateCentroidShift(ArrayList<Centroid> currentCentroids, Configuration conf, int iteration)
            throws IOException {

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

}
