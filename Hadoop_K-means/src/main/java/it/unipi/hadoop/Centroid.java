package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class Centroid implements WritableComparable<Centroid> {

    /**
     * Object Point representing the centroid
     */
    private Point point;

    /**
     * Centroid id
     */
    private final IntWritable centroid_id;

    /**
     * Constructor
     *
     * @param centroid_id: the centroid id
     * @param coords:      array of double representing the centroid's coordinates
     */
    public Centroid(int centroid_id, ArrayList<Double> coords) {
        this.centroid_id = new IntWritable(centroid_id);
        this.point = new Point(coords);
    }

    /**
     * Constructor
     */
    public Centroid() {
        this.centroid_id = new IntWritable();
        this.point = new Point();
    }

    /* WritableComparable implementation */

    /**
     * write method implementation of Writable interface
     *
     * @param dataOutput: output data
     * @throws IOException: exception in the I/O operation
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.centroid_id.write(dataOutput);
        this.point.write(dataOutput);
    }

    /**
     * compareTo method implementation of Comparable interface;
     * this method is used for sorting the ArrayList of centroids starting from the id
     *
     * @param other: the object to be compared.
     * @return result of the comparison of the two id
     */
    @Override
    public int compareTo(Centroid other) {
        return Double.compare(this.getCentroid_id().get(), other.getCentroid_id().get());
    }

    /**
     * readFields method implementation of Writable interface
     *
     * @param dataInput: input data
     * @throws IOException: exception in the I/O operation
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.centroid_id.readFields(dataInput);
        this.point.readFields(dataInput);
    }

    /* Getters and setters */

    /**
     * @return the object Point representing the centroid
     */
    public Point getPoint() {
        return point;
    }

    /**
     * @param point: the object Point representing the centroid
     */
    public void setPoint(Point point) {
        this.point = point;
    }

    /**
     * @return the centroid id
     */
    public IntWritable getCentroid_id() {
        return centroid_id;
    }

}
