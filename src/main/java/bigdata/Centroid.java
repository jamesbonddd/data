package bigdata;

import com.google.gson.Gson;

public class Centroid {

	private PointAcc point;
	private int id;
	
	public PointAcc getPoint() {
		return point;
	}
	
	public Centroid(PointAcc point , int id) {
		this.point = point;
		this.id = id;
	}	
	
	public String toString() {
		return (new Gson()).toJson(this);
	}
	
	public int getId()	{
		return id;
	}
	
}
