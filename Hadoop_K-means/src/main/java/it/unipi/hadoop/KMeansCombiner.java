package it.unipi.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
	@Override
	protected void reduce(IntWritable centroidId, Iterable<Point> points, Context context) throws IOException, InterruptedException {

		final Iterator<Point> it = points.iterator();
		Point partialSum = it.next();
		while (it.hasNext()) {
			// Add every coordinate of the points
			partialSum.add(it.next());
		}
		context.write(centroidId, partialSum);
	}
}