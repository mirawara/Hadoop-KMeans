package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;

//TODO: gestione delle eccezioni
public class KMeans {

    private static void KMeansIterations(Configuration conf, Path outputPath, Path inputPath, int numReducers) throws IOException {

        FileSystem fs = FileSystem.get(conf);

        boolean converged = false;
        int iteration = 0;

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

            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            }


            //TODO: non conviene fare il merge ma utilizzare un metodo per leggere i file dei reducer (più efficiente e anche più semplice)
            if (numReducers > 1) {
                KMeansUtil.mergeOutput(conf, outputPath, iteration);
            } else {
                //TODO: togliere il renaming perché abbiamo già il log, è solo una perdita di tempo
                fs.rename(new Path(outputPath + "/part-r-00000"), new Path(outputPath + KMeansUtil.OUTPUT_NAME + iteration));
            }

            // Read current centroids
            String currentCentroidFile = outputPath + KMeansUtil.OUTPUT_NAME + iteration;
            ArrayList<Centroid> currentCentroids = KMeansUtil.readCentroids(currentCentroidFile, conf, false);
            double shift = KMeansUtil.calculateCentroidShift(currentCentroids, conf);
            converged = (shift < KMeansUtil.DEFAULT_THRESHOLD);

            try {
                KMeansUtil.logIterationInfo(iteration, shift);
            } catch (IOException e) {
                System.err.println("Error during the write on the MapReduce log file");
                e.printStackTrace();
            }

            if (!converged) {
                KMeansUtil.setCentroidsToConf("centroids", currentCentroids, conf);
            }
            iteration++;
        }
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

        // Set centroids in the configuration

        KMeansUtil.setCentroidsToConf("centroids", KMeansUtil.readCentroids(centroidPath, conf, true), conf);

        KMeansIterations(conf, outputPath, inputPath, numReducers);

    }
}
