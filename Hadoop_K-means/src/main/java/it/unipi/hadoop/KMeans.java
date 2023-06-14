package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;


public class KMeans {


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

        // Set centroid path in the configuration
        conf.set("centroidPath", centroidPath);

        boolean converged = false;
        int iteration = 0;
        
        fs.delete(outputPath,true);
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

            if (numReducers > 1) {
                KMeansUtil.mergeOutput(conf, outputPath, iteration);
            } else {
                fs.rename(new Path(outputPath, "part-00000"), new Path(outputPath, KMeansUtil.OUTPUT_NAME + iteration));
            }
            // Check convergence
            double shift = KMeansUtil.calculateCentroidShift( centroidPath, outputPath, iteration);
            converged = (shift < KMeansUtil.DEFAULT_THRESHOLD);

            iteration++;
        }

        KMeansUtil.cleanup(iteration,fs,outputPath);

    }


}
