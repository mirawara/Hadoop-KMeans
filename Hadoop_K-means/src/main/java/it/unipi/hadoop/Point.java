package main.java.it.unipi.hadoop;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class Point implements WritableComparable<Point> {
	
	private ArrayList<Double> coordinates;
	
	/**
	 * Constructor for point class
	 *
	 * @param coordinates_ Coordinates of the new point
	 */
	public Point(ArrayList<Double> coordinates_) {
		this.coordinates = coordinates_;
	}
	
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
	}
	
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
	
	public ArrayList<Double> getCoordinates() {
		return coordinates;
	}
	
	public void setCoordinates(ArrayList<Double> coordinates) {
		this.coordinates = coordinates;
	}
}
