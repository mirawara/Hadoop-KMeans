package main.java.it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid implements WritableComparable<Centroid> {

	private Point point;
	
	private IntWritable centroid_id;
	
	/* WritableComparable implementation */
	
	@Override
	public int compareTo(Centroid centroid) {
		return centroid.centroid_id.compareTo(this.centroid_id);
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.centroid_id.write(dataOutput);
		this.point.write(dataOutput);
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.centroid_id.readFields(dataInput);
		this.point.readFields(dataInput);
	}
	
	/* Getters and setters */
	
	public Point getPoint() {
		return point;
	}
	
	public void setPoint(Point point) {
		this.point = point;
	}
	
	public IntWritable getCentroid_id() {
		return centroid_id;
	}
	
	public void setCentroid_id(IntWritable centroid_id) {
		this.centroid_id = centroid_id;
	}
	
}
