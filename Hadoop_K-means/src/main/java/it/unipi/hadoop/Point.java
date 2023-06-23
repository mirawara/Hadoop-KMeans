package it.unipi.hadoop;

import com.sun.istack.NotNull;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Represents a point in the KMeans algorithm.
 * Each point is characterized by its coordinates and can perform various operations such as distance calculation, addition, and averaging.
 */
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
     * Constructor for creating a Point object with the given coordinates.
     *
     * @param coordinates_ Coordinates of the new point.
     */
    public Point(ArrayList<Double> coordinates_) {
        this.coordinates = coordinates_;
        this.instances = 1;
    }

    /**
     * Default constructor for a Point object.
     * Initializes the coordinates to an empty ArrayList and the instances to 1.
     */
    public Point() {
        this.coordinates = new ArrayList<>();
        this.instances = 1;
    }

    /**
     * Constructor for creating a Point object from a comma-separated string representation.
     *
     * @param text The input text representing the coordinates.
     * @throws NullPointerException If the text is null.
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
     * Writes the Point object to a DataOutput stream.
     *
     * @param dataOutput The output stream to write the data to.
     * @throws IOException If an I/O error occurs.
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
     * Reads the Point object from a DataInput stream.
     *
     * @param dataInput The input stream to read the data from.
     * @throws IOException If an I/O error occurs.
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
     * Calculates the Euclidean distance between two points.
     *
     * @param point The point from which you want to calculate the distance.
     * @return The Euclidean distance between the two points.
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
     * Adds two points coordinate by coordinate.
     *
     * @param point The point to be added to the current one.
     */
    public void add(Point point) {
        for (int i = 0; i < this.coordinates.size(); i++) {
            this.coordinates.set(i, this.coordinates.get(i) + point.getCoordinates().get(i));
        }
        this.instances += point.getInstances();
    }

    /**
     * Calculates the average of the points' sum.
     * Divides each coordinate by the number of instances.
     */
    public void average() {
        this.coordinates.replaceAll(aDouble -> aDouble / this.instances);
        this.instances = 1;
    }

    /* Getters and setters */

    /**
     * Retrieves the ArrayList of doubles representing the coordinates.
     *
     * @return The ArrayList of doubles representing the coordinates.
     */
    public ArrayList<Double> getCoordinates() {
        return coordinates;
    }

    /**
     * Retrieves the number of instances of the partial sum.
     *
     * @return The number of instances of the partial sum.
     */
    public int getInstances() {
        return instances;
    }

    /**
     * Returns a string representation of the Point object.
     *
     * @return The string representation of the Point object.
     */
    public String toString() {
        return this.coordinates.stream()
                .map(Object::toString)
                .collect(Collectors.joining(" "));
    }
}
