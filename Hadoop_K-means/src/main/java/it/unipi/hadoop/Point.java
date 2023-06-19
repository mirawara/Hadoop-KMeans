package it.unipi.hadoop;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class Point implements Writable {

    /**
     * ArrayList of doubles representing the point's coordinates
     */
    private ArrayList<Double> coordinates;

    /**
     * If instances > 1, the point is a partialSum and this number represents
     * the number of points that have been added up. It is useful for the average.
     */
    private int instances;

    /**
     * Constructor
     *
     * @param coordinates_ Coordinates of the new point
     */
    public Point(ArrayList<Double> coordinates_) {
        this.coordinates = coordinates_;
        this.instances = 1;
    }

    /**
     * Constructor
     */
    public Point() {
        this.coordinates = new ArrayList<>();
        this.instances = 1;
    }

    /**
     * Constructor
     *
     * @param text: Text input to the map function
     * @throws NullPointerException: in the case text is null
     */
    public Point(String text) throws NullPointerException {
        String[] c = text.split(",");
        this.coordinates = new ArrayList<>();
        for (String x : c) {
            this.coordinates.add(Double.parseDouble(x));
        }
        this.instances = 1;
    }

    /* Writable implementation */

    /**
     * @param dataOutput: output data
     * @throws IOException: exception in the I/O operation
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.coordinates.size());
        for (Double coordinate : this.coordinates) {
            dataOutput.writeDouble(coordinate);
        }
        dataOutput.writeInt(instances);
    }

    /**
     * @param dataInput: input data
     * @throws IOException: exception in the I/O operation
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.coordinates = new ArrayList<>();
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            double value = dataInput.readDouble();
            this.coordinates.add(value);
        }
        this.instances = dataInput.readInt();
    }

    /* Operations on points */

    /**
     * It calculates the Euclidean distance between two points
     *
     * @param point: the point from which you want to calculate the distance
     */
    public double getDistance(@NotNull Point point) {
        double sum = 0;
        for (int i = 0; i < coordinates.size(); i++) {
            double diff = this.coordinates.get(i) - point.coordinates.get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * It adds two points, coordinate by coordinate
     *
     * @param point: the point to be added to the current one
     */
    public void add(Point point) {
        for (int i = 0; i < this.coordinates.size(); i++) {
            this.coordinates.set(i, this.coordinates.get(i) + point.getCoordinates().get(i));
        }
        this.instances += point.getInstances();
    }

    /**
     * It calculates the average of the points sum
     */
    public void average() {
        this.coordinates.replaceAll(aDouble -> aDouble / this.instances);
        this.instances = 1;
    }

    /* Getters and setters */

    /**
     * @return the ArrayList of doubles representing the coordinates
     */
    public ArrayList<Double> getCoordinates() {
        return coordinates;
    }

    /**
     * @return the number of instances of the partialSum.
     */
    public int getInstances() {
        return instances;
    }

    /**
     * @return String representation of the object
     */
    public String toString() {
        return this.coordinates.stream()
                .map(Object::toString)
                .collect(Collectors.joining(" "));
    }
}
