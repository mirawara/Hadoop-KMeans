package main.java.it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class PointSum implements WritableComparable<Point> {
	
	private final ArrayList<Double> summedCoordinates;
	
	private int instances;
	
	public PointSum(int dimension){
		this.instances = 0;
		summedCoordinates = new ArrayList<Double>();
		for(int i = 0; i < dimension; i++){
			this.summedCoordinates.add(0.0);
		}
	}
	
	/*** WRITABLE-COMPARABLE functions ***/
	
	@Override
	public int compareTo(Point point) {
		for (int i = 0; i < this.summedCoordinates.size(); i++) {
			if (this.summedCoordinates.get(i) < point.getCoordinates().get(i)) {
				return -1;
			} else if (this.summedCoordinates.get(i) > point.getCoordinates().get(i)) {
				return 1;
			}
		}
		return 0;
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(this.summedCoordinates.size());
		for (Double coordinate : this.summedCoordinates){
			dataOutput.writeDouble(coordinate);
		}
		dataOutput.writeInt(instances);
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		for (int i = 0; i < dataInput.readInt(); i++) {
			double value = dataInput.readDouble();
			this.summedCoordinates.add(value);
		}
		this.instances = dataInput.readInt();
	}
	
	/*** ADD AND AVERAGE ***/
	
	public void add(Point point){
		for (int i = 0; i < this.summedCoordinates.size(); i++){
			this.summedCoordinates.set(i, this.summedCoordinates.get(i) + point.getCoordinates().get(i));
			System.out.print(this.summedCoordinates.get(i) + " ");
		}
		this.instances += 1;
	}
	
	public void add(PointSum summedPoint){
		for (int i = 0; i < this.summedCoordinates.size(); i++){
			this.summedCoordinates.set(i, this.summedCoordinates.get(i) + summedPoint.getSummedCoordinates().get(i));
			System.out.print(this.summedCoordinates.get(i) + " ");
		}
		this.instances += summedPoint.getInstances();
	}
	
	public void average(){
		this.summedCoordinates.replaceAll(aDouble -> aDouble / this.instances);
	}
	
	/*** GETTERS ***/
	
	public ArrayList<Double> getSummedCoordinates() {
		return summedCoordinates;
	}
	
	public int getInstances() {
		return instances;
	}
}
