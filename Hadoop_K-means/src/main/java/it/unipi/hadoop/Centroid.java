package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Represents a centroid in the KMeans algorithm.
 * Each centroid is associated with a centroid ID and a Point object representing its coordinates.
 */
public class Centroid implements WritableComparable<Centroid> {

    /**
     * Object Point representing the centroid.
     */
    private Point point;

    /**
     * Centroid ID.
     */
    private final IntWritable centroid_id;

    /**
     * Constructor for creating a Centroid object with the given centroid ID and coordinates.
     *
     * @param centroid_id The centroid ID.
     * @param coords      An ArrayList of Double values representing the centroid's coordinates.
     */
    public Centroid(int centroid_id, ArrayList<Double> coords) {
        this.centroid_id = new IntWritable(centroid_id);
        this.point = new Point(coords);
    }

    /**
     * Default constructor for a Centroid object.
     * Initializes the centroid ID and Point object to default values.
     */
    public Centroid() {
        this.centroid_id = new IntWritable();
        this.point = new Point();
    }

    /* WritableComparable implementation */

    /**
     * Writes the Centroid object to a DataOutput stream.
     *
     * @param dataOutput The output stream to write the data to.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.centroid_id.write(dataOutput);
        this.point.write(dataOutput);
    }

    /**
     * Compares this Centroid object with another Centroid object for sorting purposes.
     * The comparison is based on the centroid IDs.
     *
     * @param other The Centroid object to be compared.
     * @return A negative integer, zero, or a positive integer if this object is less than, equal to, or greater than the other object.
     */
    @Override
    public int compareTo(Centroid other) {
        return Double.compare(this.getCentroid_id().get(), other.getCentroid_id().get());
    }

    /**
     * Reads the Centroid object from a DataInput stream.
     *
     * @param dataInput The input stream to read the data from.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.centroid_id.readFields(dataInput);
        this.point.readFields(dataInput);
    }

    /* Getters and setters */

    /**
     * Retrieves the Point object representing the centroid.
     *
     * @return The Point object representing the centroid.
     */
    public Point getPoint() {
        return point;
    }

    /**
     * Sets the Point object representing the centroid.
     *
     * @param point The Point object representing the centroid.
     */
    public void setPoint(Point point) {
        this.point = point;
    }

    /**
     * Retrieves the centroid ID.
     *
     * @return The centroid ID.
     */
    public IntWritable getCentroid_id() {
        return centroid_id;
    }

}
