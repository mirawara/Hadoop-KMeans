package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * The KMeansCombiner class extends the Reducer class and is used to combine the intermediate results of the KMeans clustering algorithm.
 */
public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

	/**
	 * This method sum the coordinates of the points, associated with a specific centroid, in order to calculate their partial sum.
	 *
	 * @param centroidId id the ID of the centroid to which the points are associated.
	 * @param points is an Iterable of the points associated with the specified centroid.
	 * @param context is the Hadoop Context object that allows the combiner to interact with the rest of the Hadoop system.
	 * @throws IOException if an I/O error occurs during the execution of the method.
	 * @throws InterruptedException if the thread is interrupted during the execution of the method.
	 */
	@Override
	protected void reduce(IntWritable centroidId, Iterable<Point> points, Context context) throws IOException, InterruptedException {
		// Create an iterator for the points
		final Iterator<Point> it = points.iterator();
		// Get the first point and use it as the initial partial sum
		Point partialSum = it.next();
		// Iterate over the remaining points
		while (it.hasNext()) {
			// call the method that sum the coordinates of the points
			partialSum.add(it.next());
		}
		// Write the centroid ID and the partial sum to the context
		context.write(centroidId, partialSum);
	}
}