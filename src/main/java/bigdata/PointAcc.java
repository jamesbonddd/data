package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;

public class PointAcc implements Writable{
	
	private int nbPoints;
	private List<Double> coordinates;
	
	public PointAcc(int nbDimentions) {
		nbPoints = 0;
		coordinates = new ArrayList<Double>();
		for(int i =0; i<nbDimentions; i++)
			coordinates.add(new Double(0));
	}
	
	public PointAcc() {
		
	}
	
	public PointAcc(List<Double > coordinates) {
		nbPoints = 1;
		this.coordinates = coordinates;
	}
	
	public void accumulate(PointAcc p ) {
		for(int i =0; i  < p.getNumberDimentions(); i++) 
			coordinates.set(i, coordinates.get(i)+p.get(i));
		nbPoints+= p.getNumberPoints();
	}
	
	public double distanceTo(PointAcc p) throws Exception {
		double sum = 0;
		for(int i =0; i < this.getNumberDimentions(); i ++) 
			sum+= Math.pow(get(i) - p.get(i) , 2);
		return Math.sqrt(sum);
	}
	
	public double distanceTo(Centroid c) throws Exception {
		return distanceTo(c.getPoint());
	}
	
	public int getNumberPoints() {
		return nbPoints;
	}
	
	public int getNumberDimentions() {
		return coordinates.size();
	}
	
	public double get(int index) {
		return coordinates.get(index);
	}
	
	public PointAcc getCentroid() {
		List<Double> ccoordinates = new ArrayList<Double>();
		for (Double ci : coordinates) 
			ccoordinates.add(ci/nbPoints);
		return new PointAcc(ccoordinates);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(coordinates.size());
		for(int i =0; i< coordinates.size(); i++)
			out.writeDouble(coordinates.get(i));
		out.writeInt(nbPoints);
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		coordinates = new ArrayList<Double>();
		for(int i =0; i< size; i ++) 
			coordinates.add(in.readDouble());
		nbPoints = in.readInt();
	}
	
	public String toString() {
		return (new Gson()).toJson(this);
	}
	
}
