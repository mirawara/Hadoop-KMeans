package main.java.it.unipi.hadoop;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class Point implements WritableComparable<Point> {
	
	private ArrayList<Double> coordinates;
	private int instances;
	
	/**
	 * Constructor for point class
	 *
	 * @param coordinates_ Coordinates of the new point
	 */
	public Point(ArrayList<Double> coordinates_) {
		this.coordinates = coordinates_;
		this.instances = 1;
	}
	
	public Point(String text) throws NullPointerException {
		String[] c = text.split(" ");
		this.coordinates = new ArrayList<>();
		for (String x : c) {
			this.coordinates.add(Double.parseDouble(x));
		}
	}
	
	
	/* WritableComparable implementation */
	
	/**
	 * @param point the object to be compared.
	 * @return :
	 * 0 if the point is equal to the current
	 * 1 if this point is greater
	 * -1 if this point is lower
	 */
	@Override
	public int compareTo(@NotNull Point point) {
		for (int i = 0; i < this.coordinates.size(); i++) {
			if (this.coordinates.get(i) < point.coordinates.get(i)) {
				return -1;
			} else if (this.coordinates.get(i) > point.coordinates.get(i)) {
				return 1;
			}
		}
		return 0;
	}
	
	/**
	 * @param dataOutput
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(this.coordinates.size());
		for (Double coordinate : this.coordinates){
			dataOutput.writeDouble(coordinate);
		}
		dataOutput.writeInt(instances);
	}
	
	/**
	 * @param dataInput
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.coordinates = new ArrayList<>();
		for (int i = 0; i < dataInput.readInt(); i++) {
			double value = dataInput.readDouble();
			this.coordinates.add(value);
		}
		this.instances = dataInput.readInt();
	}
	
	/* Operations on points */
	
	/**
	 * @param point
	 */
	public double getDistance(@NotNull Point point) {
		double sum = 0;
		for (int i = 0; i < coordinates.size(); i++) {
			double diff = this.coordinates.get(i) - point.coordinates.get(i);
			sum += diff * diff;
		}
		return Math.sqrt(sum);
	}
	
	public void add(Point point){
		for (int i = 0; i < this.coordinates.size(); i++){
			this.coordinates.set(i, this.coordinates.get(i) + point.getCoordinates().get(i));
			System.out.print(this.coordinates.get(i) + " ");
		}
		this.instances += point.getInstances();
	}
	
	public void average(){
		this.coordinates.replaceAll(aDouble -> aDouble / this.instances);
	}
	
	/* Getters and setters */
	
	public ArrayList<Double> getCoordinates() {
		return coordinates;
	}
	
	public void setCoordinates(ArrayList<Double> coordinates) {
		this.coordinates = coordinates;
	}
	
	public int getInstances() {
		return instances;
	}
}
